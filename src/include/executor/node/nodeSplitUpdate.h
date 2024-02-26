/*-------------------------------------------------------------------------
 *
 * nodeSplitUpdate.h
 *        Prototypes for nodeSplitUpdate.
 *
 * Portions Copyright (c) 2012, EMC Corp.
 * Portions Copyright (c) 2012-2022 VMware, Inc. or its affiliates.
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *     src/include/executor/node/nodeSplitUpdate.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef OPENGAUSS_SERVER_NODESPLITUPDATE_H
#define OPENGAUSS_SERVER_NODESPLITUPDATE_H

#ifdef USE_SPQ
#include "nodes/execnodes.h"

extern SplitUpdateState* ExecInitSplitUpdate(SplitUpdate *node, EState *estate, int eflags);
extern void ExecEndSplitUpdate(SplitUpdateState *node);
#endif /* USE_SPQ */

#endif //OPENGAUSS_SERVER_NODESPLITUPDATE_H
