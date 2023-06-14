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
 * txn_redo.cpp
 *      TxnRedoWorker runs in the dispatcher thread to easy the management
 *      of transaction status and global variables.  In principle, we can
 *      run the TxnRedoWorker in a separate thread, but we don't do it for
 *      now for simplicity.
 *      To ensure read consistency on hot-standby replicas, transactions on
 *      replicas must commit in the same order as the master.  This is the
 *      main reason to use a dedicated worker to replay transaction logs.
 *      To ensure data consistency within a transaction, the transaction
 *      commit log must be replayed after all data logs for the transaction
 *      have been replayed by PageRedoWorkers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/ondemand_extreme_rto/txn_redo.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/startup.h"
#include "access/xlog.h"
#include "utils/palloc.h"
#include "utils/guc.h"
#include "portability/instr_time.h"

#include "access/ondemand_extreme_rto/dispatcher.h"
#include "access/ondemand_extreme_rto/txn_redo.h"
#include "access/xlogreader.h"
#include "pgstat.h"
#include "storage/standby.h"
#include "catalog/pg_control.h"

namespace ondemand_extreme_rto {

void AddTxnRedoItem(PageRedoWorker *worker, void *item)
{
    (void)SPSCBlockingQueuePut(worker->queue, item);
}

}  // namespace ondemand_extreme_rto
