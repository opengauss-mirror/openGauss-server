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
 * txn_redo.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/parallel_recovery/txn_redo.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLEL_RECOVERY_TXN_REDO_H
#define PARALLEL_RECOVERY_TXN_REDO_H

#include "access/parallel_recovery/redo_item.h"
namespace parallel_recovery {

typedef struct TxnRedoWorker TxnRedoWorker;

TxnRedoWorker* StartTxnRedoWorker();
void DestroyTxnRedoWorker(TxnRedoWorker* worker);

void AddTxnRedoItem(TxnRedoWorker* worker, RedoItem* item);
void ApplyReadyTxnLogRecords(TxnRedoWorker* worker, bool forceAll);
void MoveTxnItemToApplyQueue(TxnRedoWorker* worker);
void DumpTxnWorker(TxnRedoWorker* txnWorker);
bool IsTxnWorkerIdle(TxnRedoWorker* worker);
XLogRecPtr getTransedTxnLsn(TxnRedoWorker *worker);
XLogRecPtr getTryingTxnLsn(TxnRedoWorker *worker);
}
#endif
