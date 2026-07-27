/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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
 * IDENTIFICATION
 *        src/include/postmaster/atfworker.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ATF_WORKER_H
#define ATF_WORKER_H

#include "postgres.h"

#define ATF_TASK_CHECK_INTERVAL_USEC 100000L

extern void WaitForAtfTaskDone();
extern bool IsAtfRecoveryDone();
extern bool MarkAtfRecoveryDone();
extern void AtfWorkerMain();
extern bool PrepareAtfRecoveryStage();

#endif /* ATF_WORKER_H */
