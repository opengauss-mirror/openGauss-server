/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
 * ss_ub_link_status.h
 *  UB (Unified Buffer) cache link availability module.
 *
 *  Minimal interface: a single pure decision function that tells whether the
 *  UB cache link is currently usable, plus a collector implemented in
 *  ss_transaction.cpp. The decision logic is dependency-free (only standard
 *  headers) so it can be unit-tested standalone.
 *
 *  The UB link is usable only when all of the following hold:
 *    - the feature is configured (enable_ub on and not in initdb)
 *    - no SIGBUS fault has disabled UB memory access (UBMemAccessEnabled)
 *    - the shared memory region and all component buffers are ready
 *    - the cluster is not in reform (UB reads are bypassed during reform)
 *
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_ub_link_status.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SS_UB_LINK_STATUS_H
#define SS_UB_LINK_STATUS_H

#include <stdbool.h>

/*
 * Pure decision function: whether the UB cache link is currently usable.
 * available = configured && !inReform && memAccess && shmemReady
 */
static inline bool SSUbLinkResolveAvailable(bool configured, bool memAccess, bool shmemReady, bool inReform)
{
    return configured && !inReform && memAccess && shmemReady;
}

/*
 * Collect the real-time availability of the whole UB cache link
 * (CLOG/CSNLOG/Snapshot/OldestXmin shared memory buffers). Implemented in
 * ss_transaction.cpp.
 */
extern bool SSIsUbLinkAvailable(void);

#endif /* SS_UB_LINK_STATUS_H */
