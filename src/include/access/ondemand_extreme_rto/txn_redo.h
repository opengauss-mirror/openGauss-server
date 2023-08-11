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
 *        src/include/access/ondemand_extreme_rto/txn_redo.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ONDEMAND_EXTREME_RTO_TXN_REDO_H
#define ONDEMAND_EXTREME_RTO_TXN_REDO_H

#include "access/parallel_recovery/redo_item.h"

namespace ondemand_extreme_rto {
void AddTxnRedoItem(PageRedoWorker *worker, void *item);
void TrxnMngProc(RedoItem *item, PageRedoWorker *wk);
void TrxnWorkerProc(RedoItem *item);
}  // namespace ondemand_extreme_rto
#endif
