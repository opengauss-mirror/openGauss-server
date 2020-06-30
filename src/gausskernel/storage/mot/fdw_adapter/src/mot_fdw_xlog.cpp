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
 *    src/gausskernel/storage/mot/fdw_adapter/src/mot_fdw_xlog.cpp
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
#include "recovery_manager.h"

extern int MOTXlateRecoveryErr(int err);

bool IsValidEntry(uint8 code)
{
    return code == MOT_REDO_DATA;
}

void RedoTransactionCommit(TransactionId xid)
{
    MOT::GetRecoveryManager()->CommitRecoveredTransaction((uint64_t)xid);
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
        elog(ERROR, "MOTRedo: invalid op code %u", recordType);
    }
    if (MOT::GetRecoveryManager()->IsErrorSet() ||
        !MOT::GetRecoveryManager()->ApplyLogSegmentFromData(lsn, data, len)) {
        // we treat errors fatally.
        ereport(FATAL,
            (MOTXlateRecoveryErr(MOT::GetRecoveryManager()->GetErrorCode()),
                errmsg("%s", MOT::GetRecoveryManager()->GetErrorString())));
    }
}

uint64_t XLOGLogger::AddToLog(uint8_t* data, uint32_t size)
{
    XLogBeginInsert();
    XLogRegisterData((char*)data, size);
    XLogInsert(RM_MOT_ID, MOT_REDO_DATA);
    return size;
}

uint64_t XLOGLogger::AddToLog(MOT::RedoLogBuffer* redoBuffer)
{
    uint32_t length;
    uint8_t* data = redoBuffer->Serialize(&length);
    return AddToLog(data, length);
}

uint64_t XLOGLogger::AddToLog(MOT::RedoLogBuffer** redoTransactionArray, uint32_t size)
{
    uint32_t written = 0;
    // ensure that we have enough space to add all transaction buffers
    XLogEnsureRecordSpace(0, size);
    XLogBeginInsert();
    for (uint32_t i = 0; i < size; i++) {
        uint32_t length;
        uint8_t* data = redoTransactionArray[i]->Serialize(&length);
        XLogRegisterData((char*)data, length);
        written += length;
    }
    XLogInsert(RM_MOT_ID, MOT_REDO_DATA);
    return written;
}

void XLOGLogger::FlushLog()
{}

void XLOGLogger::CloseLog()
{}

void XLOGLogger::ClearLog()
{}
