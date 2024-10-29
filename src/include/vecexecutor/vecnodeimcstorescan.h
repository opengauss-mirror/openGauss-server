/*
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
 * ---------------------------------------------------------------------------------------
 *
 * vecnodeimcstorescan.h
 *
 *
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnodeimcstorescan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODEIMCSTORESCAN_H
#define VECNODEIMCSTORESCAN_H

#include "postgres.h"
#include "vecexecutor/vecnodes.h"
#include "nodes/plannodes.h"
#include "access/cstoreskey.h"
#include "vecexecutor/vecexecutor.h"
#include "executor/executor.h"
#include "utils/memutils.h"

#ifdef ENABLE_HTAP
typedef CStoreScanState IMCStoreScanState;

extern IMCStoreScanState* ExecInitIMCStoreScan(IMCStoreScan* node, Relation parentHeapRel, EState* estate, int eflags,
    bool codegenInUplevel = false);

#endif /* ENABLE_HTAP */

#endif // VECNODEIMCSTORESCAN_H
