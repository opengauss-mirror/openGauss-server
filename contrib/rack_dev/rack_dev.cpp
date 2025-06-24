/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "postgres.h"

#include "funcapi.h"
#include "utils/builtins.h"
#include "access/smb.h"
#include "catalog/pg_type.h"
#include "utils/elog.h"
#include "rack_mem_tuple_desc.h"
#include "rack_dev.h"

PG_MODULE_MAGIC;

static void GetSMBBufMgrLsnAndUsedbufs(smb_recovery::SMBBufMetaMem* mgr, int id, XLogRecPtr& headLsn,
                                       XLogRecPtr& tailLsn, int& usedBufs)
{
    int current;
    int lruTailId;

    current = *mgr[id].lruHeadId;
    lruTailId = *mgr[id].lruTailId;
    while (current != lruTailId && XLogRecPtrIsInvalid(smb_recovery::SMBWriterIdGetItem(mgr, current)->lsn)) {
        current = smb_recovery::SMBWriterIdGetItem(mgr, current)->nextLRUId;
    }
    headLsn = smb_recovery::SMBWriterIdGetItem(mgr, current)->lsn;
    tailLsn = smb_recovery::SMBWriterIdGetItem(mgr, lruTailId)->lsn;

    while (current != SMB_INVALID_ID) {
        usedBufs++;
        current = smb_recovery::SMBWriterIdGetItem(mgr, current)->nextLRUId;
    }
}

static void GetSMBLsnAndUsedBuffers(smb_recovery::SMBBufMetaMem* mgr, XLogRecPtr& startLsn,
                                    XLogRecPtr& endLsn, int& usedBufs)
{
    for (int i = 0; i < smb_recovery::SMB_BUF_MGR_NUM; i++) {
        XLogRecPtr headLsn = InvalidXLogRecPtr;
        XLogRecPtr tailLsn = InvalidXLogRecPtr;
        GetSMBBufMgrLsnAndUsedbufs(mgr, i, headLsn, tailLsn, usedBufs);
        startLsn = Max(startLsn, headLsn);
        endLsn = Min(endLsn, tailLsn);
    }
}

PG_FUNCTION_INFO_V1(smb_total_buffer_info);

Datum smb_total_buffer_info(PG_FUNCTION_ARGS)
{
    XLogRecPtr startLsn = InvalidXLogRecPtr;
    XLogRecPtr endLsn = MAX_XLOG_REC_PTR;
    const int maxLsnLen = 64;
    const int maxRateLen = 8;
    char startLsnChar[maxLsnLen];
    char endLsnChar[maxLsnLen];
    int usedBufs = 0;
    int totalBufs;
    float bufUsedRate;
    char bufUsedRateChar[maxLsnLen];

    const int cols = 5;
    uint32 shiftSize = 32;
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum values[cols];
    Datum result;
    errno_t rc = EOK;

    TupleDescEntry entries[cols] = {{"smb_start_lsn",         TEXTOID},
                                    {"smb_end_lsn",           TEXTOID},
                                    {"total_used_page_num",   INT8OID},
                                    {"total_page_num",        INT8OID},
                                    {"total_buffer_use_rate", TEXTOID}};

    smb_recovery::SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    GetSMBLsnAndUsedBuffers(mgr, startLsn, endLsn, usedBufs);
    totalBufs = g_instance.smb_cxt.NSMBBuffers;
    bufUsedRate = static_cast<float>(usedBufs) / totalBufs * 100.0f;

    rc = snprintf_s(startLsnChar, sizeof(startLsnChar), sizeof(startLsnChar) - 1, "%X/%X",
                    static_cast<uint32>(startLsn >> shiftSize), static_cast<uint32>(startLsn));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(endLsnChar, sizeof(endLsnChar), sizeof(endLsnChar) - 1, "%X/%X",
                    static_cast<uint32>(endLsn >> shiftSize), static_cast<uint32>(endLsn));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(bufUsedRateChar, sizeof(bufUsedRateChar), sizeof(bufUsedRateChar) - 1, "%.2f%%", bufUsedRate);
    securec_check_ss(rc, "\0", "\0");

    tupdesc = construct_tuple_desc(entries, cols);
    values[ARR_0] = CStringGetTextDatum(startLsnChar);
    values[ARR_1] = CStringGetTextDatum(endLsnChar);
    values[ARR_2] = UInt64GetDatum(usedBufs);
    values[ARR_3] = UInt64GetDatum(totalBufs);
    values[ARR_4] = CStringGetTextDatum(bufUsedRateChar);

    bool nulls[cols] = {false, false, false, false, false};

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(smb_buf_manager_info);

Datum smb_buf_manager_info(PG_FUNCTION_ARGS)
{
    uint32 bufMgrId = PG_GETARG_UINT32(0);
    XLogRecPtr headLsn;
    XLogRecPtr tailLsn;
    const int maxLsnLen = 64;
    const int maxRateLen = 8;
    char headLsnChar[maxLsnLen];
    char tailLsnChar[maxLsnLen];
    int usedBufs = 0;
    int totalBufs;
    float bufUsedRate;
    char bufUsedRateChar[maxLsnLen];

    const int cols = 5;
    uint32 shiftSize = 32;
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum values[cols];
    Datum result;
    errno_t rc = EOK;

    TupleDescEntry entries[cols] = {{"buf_manager_head_lsn", TEXTOID},
                                    {"buf_manager_tail_lsn", TEXTOID},
                                    {"used_page_num",        INT8OID},
                                    {"total_page_num",       INT8OID},
                                    {"buffer_use_rate",      TEXTOID}};

    smb_recovery::SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    GetSMBBufMgrLsnAndUsedbufs(mgr, bufMgrId, headLsn, tailLsn, usedBufs);
    totalBufs = SMB_WRITER_ITEM_PER_MGR;
    bufUsedRate = static_cast<float>(usedBufs) / totalBufs * 100.0f;

    rc = snprintf_s(headLsnChar, sizeof(headLsnChar), sizeof(headLsnChar) - 1, "%X/%X",
                    static_cast<uint32>(headLsn >> shiftSize), static_cast<uint32>(headLsn));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(tailLsnChar, sizeof(tailLsnChar), sizeof(tailLsnChar) - 1, "%X/%X",
                    static_cast<uint32>(tailLsn >> shiftSize), static_cast<uint32>(tailLsn));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(bufUsedRateChar, sizeof(bufUsedRateChar), sizeof(bufUsedRateChar) - 1, "%.2f%%", bufUsedRate);
    securec_check_ss(rc, "\0", "\0");

    tupdesc = construct_tuple_desc(entries, cols);
    values[ARR_0] = CStringGetTextDatum(headLsnChar);
    values[ARR_1] = CStringGetTextDatum(tailLsnChar);
    values[ARR_2] = UInt64GetDatum(usedBufs);
    values[ARR_3] = UInt64GetDatum(totalBufs);
    values[ARR_4] = CStringGetTextDatum(bufUsedRateChar);

    bool nulls[cols] = {false, false, false, false, false};

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(smb_dirty_page_queue_info);

Datum smb_dirty_page_queue_info(PG_FUNCTION_ARGS)
{
    uint32 queueId = PG_GETARG_UINT32(0);
    uint64 usedNum;
    const int cols = 5;
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum values[cols];
    Datum result;

    TupleDescEntry entries[cols] = {{"total_inserted_page_nums",  INT8OID},
                                    {"dirty_page_queue_size",     INT8OID},
                                    {"dirty_page_queue_head",     INT8OID},
                                    {"dirty_page_queue_tail",     INT8OID},
                                    {"dirty_page_queue_used_num", INT8OID}};

    tupdesc = construct_tuple_desc(entries, cols);
    usedNum = g_instance.smb_cxt.pageQueue[queueId].tail - g_instance.smb_cxt.pageQueue[queueId].head;
    values[ARR_0] = UInt64GetDatum(g_instance.smb_cxt.smbInfo->pageDirtyNums);
    values[ARR_1] = UInt64GetDatum(g_instance.smb_cxt.pageQueue[queueId].size);
    values[ARR_2] = UInt64GetDatum(g_instance.smb_cxt.pageQueue[queueId].head);
    values[ARR_3] = UInt64GetDatum(g_instance.smb_cxt.pageQueue[queueId].tail);
    values[ARR_4] = UInt64GetDatum(usedNum);

    bool nulls[cols] = {false, false, false, false, false};

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(smb_status_info);

Datum smb_status_info(PG_FUNCTION_ARGS)
{
    const int cols = 1;
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum values[cols];
    int lruRemovedCount;
    bool nulls[cols] = {false};

    TupleDescEntry entries[cols] = {{"lru_removed_num", INT4OID, -1, 0}};

    tupdesc = construct_tuple_desc(entries, cols);

    if (g_instance.smb_cxt.smbInfo == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("SMB is not enabled"),
                        erraction("Start SMB firstly")));
    }

    lruRemovedCount = g_instance.smb_cxt.smbInfo->lruRemovedNum;
    values[ARR_0] = Int32GetDatum(lruRemovedCount);
    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}