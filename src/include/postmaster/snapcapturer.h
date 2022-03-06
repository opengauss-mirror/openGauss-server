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
 * -------------------------------------------------------------------------
 * 
 * IDENTIFICATION
 *        src/include/postmaster/snapcapturer.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SNAPCAPTURER_H
#define SNAPCAPTURER_H

#include "utils/snapshot.h"

typedef enum TxnWorkerStatus {
    TXNWORKER_DEFAULT,
    TXNWORKER_STARTED,
    TXNWORKER_DONE
} TxnWorkerStatus;

typedef struct TxnSnapWorkerInfo {
    NameData dbName;
    ThreadId snapworkerPid; /* PID (0 if not started) */
    TxnWorkerStatus status;
    Latch latch;
} TxnSnapWorkerInfo;
typedef struct TxnSnapCapShmemStruct {
    TxnSnapWorkerInfo workerInfo;
} TxnSnapCapShmemStruct;

extern Size TxnSnapCapShmemSize(void);
extern void TxnSnapCapShmemInit(void);

extern void TxnSnapDeserialize(char *buf, Snapshot snap);

extern ThreadId StartTxnSnapCapturer(void);
extern bool IsTxnSnapCapturerProcess(void);
extern NON_EXEC_STATIC void TxnSnapCapturerMain(void);

extern ThreadId StartSnapWorkerCapturer(void);
extern bool IsTxnSnapWorkerProcess(void);
extern NON_EXEC_STATIC void TxnSnapWorkerMain(void);

#define ENABLE_TCAP_VERSION (u_sess->attr.attr_storage.undo_retention_time > 0)

#endif
