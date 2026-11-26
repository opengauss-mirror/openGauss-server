/* -------------------------------------------------------------------------
 *
 * undologdesc.cpp
 *    c++ code
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/rmgrdesc
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "access/ustore/undo/knl_uundoxlog.h"

const char* undo::undo_xlog_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_UNDO_UNLINK:
        {
            return "undo_unlink";
            break;
        }
        case XLOG_UNDO_EXTEND:
        {
            return "undo_extend";
            break;
        }
        case XLOG_UNDO_DISCARD:
        {
            return "undo_slot_discard";
            break;
        }
        case XLOG_SLOT_EXTEND:
        {
            return "undo_slot_extend";
            break;
        }
        case XLOG_SLOT_UNLINK:
        {
            return "undo_slot_unlink";
            break;
        }
        default:
            break;
    }
    return "unknown_type";
}

void undo::UndoXlogDesc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_UNDO_UNLINK:
        {
            XlogUndoUnlink *xlrec = (XlogUndoUnlink *) rec;
            appendStringInfo(buf, "UNLINK_UNDO_LOG: pre head zoneId/offset=%d/%lu, head zoneId/offset=%d/%lu.",
                (int)(UNDO_PTR_GET_ZONE_ID(xlrec->prevhead)), UNDO_PTR_GET_OFFSET(xlrec->prevhead),
                (int)(UNDO_PTR_GET_ZONE_ID(xlrec->head)), UNDO_PTR_GET_OFFSET(xlrec->head));
            break;
        }
        case XLOG_UNDO_EXTEND:
        {
            XlogUndoExtend *xlrec = (XlogUndoExtend *) rec;
            appendStringInfo(buf, "EXTEND_UNDO_LOG: pre tail zoneId/offset=%d/%lu, tail zoneId/offset=%d/%lu.",
                (int)(UNDO_PTR_GET_ZONE_ID(xlrec->prevtail)), UNDO_PTR_GET_OFFSET(xlrec->prevtail),
                (int)(UNDO_PTR_GET_ZONE_ID(xlrec->tail)), UNDO_PTR_GET_OFFSET(xlrec->tail));
            break;
        }
        case XLOG_UNDO_DISCARD:
        {
            XlogUndoDiscard *xlrec = (XlogUndoDiscard *) rec;
            appendStringInfo(buf, "DISCARD_UNDO_LOG: zone %d recycle [%lu, %lu) discard %lu"
                " loops %lu xid %lu xmin %lu.",
                (int)UNDO_PTR_GET_ZONE_ID(xlrec->startSlot), xlrec->startSlot, xlrec->endSlot, xlrec->endUndoPtr,
                xlrec->recycleLoops, xlrec->recycledXid, xlrec->oldestXmin);
            break;
        }
        case XLOG_SLOT_EXTEND:
        {
            XlogUndoExtend *xlrec = (XlogUndoExtend *) rec;
            appendStringInfo(buf, "EXTEND_UNDO_SLOT_LOG: pre tail zoneId/offset=%d/%lu, tail zoneId/offset=%d/%lu.",
                (int)(UNDO_PTR_GET_ZONE_ID(xlrec->prevtail)), UNDO_PTR_GET_OFFSET(xlrec->prevtail),
                (int)(UNDO_PTR_GET_ZONE_ID(xlrec->tail)), UNDO_PTR_GET_OFFSET(xlrec->tail));
            break;
        }
        case XLOG_SLOT_UNLINK:
        {
            XlogUndoUnlink *xlrec = (XlogUndoUnlink *) rec;
            appendStringInfo(buf, "UNLINK_UNDO_SLOT_LOG: pre head zoneId/offset=%d/%lu, head zoneId/offset=%d/%lu.",
                (int)(UNDO_PTR_GET_ZONE_ID(xlrec->prevhead)), UNDO_PTR_GET_OFFSET(xlrec->prevhead),
                (int)(UNDO_PTR_GET_ZONE_ID(xlrec->head)), UNDO_PTR_GET_OFFSET(xlrec->head));
            break;
        }
        default:
        {
            appendStringInfo(buf, "UNKNOWN");
            break;
        }
    }
}

const char* undo::undo_xlog_roll_back_finish_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_ROLLBACK_FINISH) {
        return "undo_rollback_finish";
    } else {
        return "unknown_type";
    }
}


void undo::UndoXlogRollbackFinishDesc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_ROLLBACK_FINISH) {
        XlogRollbackFinish *xlrec = (XlogRollbackFinish *) rec;
        appendStringInfo(buf, "ROLLBACK FINISH: zoneId %d slot offset %lu.",
            (int)UNDO_PTR_GET_ZONE_ID(xlrec->slotPtr), UNDO_PTR_GET_OFFSET(xlrec->slotPtr));
    }
}

