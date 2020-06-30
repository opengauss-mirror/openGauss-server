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
 * instr_workload.h
 *        definitions for instruments workload
 *
 *
 * IDENTIFICATION
 *        src/include/instruments/instr_workload.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INSTR_WORKLOAD_H
#define INSTR_WORKLOAD_H
#include "c.h"
#include "utils/timestamp.h"
#include "utils/syscache.h"

typedef struct StatData {
    TimestampTz max;
    TimestampTz min;
    TimestampTz average;
    TimestampTz total;
} StatData;

typedef struct WLMTransactonInfo {
    uint64 commit_counter;
    uint64 rollback_counter;
    StatData responstime;
} WLMTransactionInfo;

typedef struct WLMWorkLoadKey {
    Oid user_id;
} WLMWorkLoadKey;

typedef struct WorkloadXactInfo {
    // WLMWorkLoadKey wlkey;
    Oid user_id;
    WLMTransactionInfo transaction_info;
    WLMTransactionInfo bg_xact_info;
} WorkloadXactInfo;

extern void InitInstrWorkloadTransaction(void);
extern void InitInstrOneUserTransaction(Oid userId);
extern void instr_report_workload_xact_info(bool isCommit);
#endif
