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
 * mot_fdw_xlog.cpp
 *    MOT Foreign Data Wrapper xlog interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw_xlog.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cstdint>
#include <iosfwd>
#include <ostream>
#include <iostream>
#include "global.h"
#include "postgres.h"
#include "access/xlog.h"
#include "mot_fdw_xlog.h"
#include "mot_engine.h"
#include "miscadmin.h"

bool IsValidEntry(uint8 code)
{
    return code == MOT_REDO_DATA;
}

void RedoTransactionCommit(TransactionId xid, void* arg)
{
    (void)MOT::GetRecoveryManager()->CommitTransaction((uint64_t)xid);
}

MOT::TxnCommitStatus GetTransactionStateCallback(uint64_t transactionId)
{
    CLogXidStatus status;
    XLogRecPtr xidlsn;
    status = CLogGetStatus((uint32_t)transactionId, &xidlsn);
    switch (status) {
        case CLOG_XID_STATUS_IN_PROGRESS:
            return MOT::TxnCommitStatus::TXN_COMMIT_IN_PROGRESS;
        case CLOG_XID_STATUS_COMMITTED:
            return MOT::TxnCommitStatus::TXN_COMMITED;
        case CLOG_XID_STATUS_ABORTED:
            return MOT::TxnCommitStatus::TXN_ABORTED;
        default:
            return MOT::TxnCommitStatus::TXN_COMMIT_INVALID;
    }
}

void MOTRedo(XLogReaderState* record)
{
    uint8 recordType = XLogRecGetInfo(record);
    char* data = XLogRecGetData(record);
    size_t len = XLogRecGetDataLen(record);
    uint64_t lsn = record->EndRecPtr;
    if (!IsValidEntry(recordType)) {
        elog(ERROR, "MOTRedo: invalid op code %" PRIu8, recordType);
    }
    if (MOT::GetRecoveryManager()->IsErrorSet() || !MOT::GetRecoveryManager()->ApplyRedoLog(lsn, data, len)) {
        // we treat errors fatally.
        ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("MOT recovery failed.")));
    }
}

uint64_t XLOGLogger::AddToLog(MOT::RedoLogBuffer** redoLogBufferArray, uint32_t size)
{
    return MOT::ILogger::AddToLog(redoLogBufferArray, size);
}

uint64_t XLOGLogger::AddToLog(uint8_t* data, uint32_t size)
{
    START_CRIT_SECTION();
    XLogBeginInsert();
    XLogRegisterData((char*)data, size);
    (void)XLogInsert(RM_MOT_ID, MOT_REDO_DATA);
    END_CRIT_SECTION();
    return size;
}

void XLOGLogger::FlushLog()
{}

void XLOGLogger::CloseLog()
{}

void XLOGLogger::ClearLog()
{}
