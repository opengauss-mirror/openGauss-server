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
 * mot_fdw_xlog.h
 *    MOT Foreign Data Wrapper xlog interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw_xlog.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_FDW_XLOG_H
#define MOT_FDW_XLOG_H

#include "ilogger.h"
#include "redo_log_buffer.h"
#include "access/clog.h"

/*
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field
 */
const int MOT_REDO_DATA = 0x10;

MOT::TxnCommitStatus GetTransactionStateCallback(uint64_t transactionId);
void RedoTransactionCommit(TransactionId xid, void* arg);

class XLOGLogger : public MOT::ILogger {
public:
    inline XLOGLogger()
    {}
    inline ~XLOGLogger()
    {}

    uint64_t AddToLog(MOT::RedoLogBuffer** redoLogBufferArray, uint32_t size);
    uint64_t AddToLog(uint8_t* data, uint32_t size);
    void FlushLog();
    void CloseLog();
    void ClearLog();
};

#endif /* MOT_FDW_XLOG_H */
