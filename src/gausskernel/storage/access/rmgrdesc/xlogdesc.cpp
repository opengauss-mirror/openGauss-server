/* -------------------------------------------------------------------------
 *
 * xlogdesc.cpp
 *	  rmgr descriptor routines for access/transam/xlog.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/xlogdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#ifdef FRONTEND
#include "common/fe_memutils.h"
#endif
#define CKPTPLUSLEN 112

/*
 * GUC support
 */
struct config_enum_entry wal_level_options[] = {
    { "minimal", WAL_LEVEL_MINIMAL, false },
    { "archive", WAL_LEVEL_ARCHIVE, false },
    { "hot_standby", WAL_LEVEL_HOT_STANDBY, false },
    { "logical", WAL_LEVEL_LOGICAL, false },
    { NULL, 0, false }
};

const char *xlog_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    switch (info) {
        case XLOG_CHECKPOINT_SHUTDOWN:
            return "shutdown_checkpoint";
            break;
        case XLOG_CHECKPOINT_ONLINE:
            return "online_checkpoint";
            break;
        case XLOG_NOOP:
            return "noop";
            break;
        case XLOG_NEXTOID:
            return "nextoid";
            break;
        case XLOG_SWITCH:
            return "xlog switch";
            break;
        case XLOG_BACKUP_END:
            return "backup end";
            break;
        case XLOG_PARAMETER_CHANGE:
            return "parameter chage";
            break;
        case XLOG_RESTORE_POINT:
            return "restore point";
            break;
        case XLOG_FPW_CHANGE:
            return "restore point";
            break;
        case XLOG_END_OF_RECOVERY:
            return "end of recovery";
            break;
        case XLOG_FPI_FOR_HINT:
            return "fpi for hint";
            break;
        case XLOG_FPI:
            return "fpi";
            break;
        case XLOG_DELAY_XLOG_RECYCLE:
            return "delay recycle";
            break;
        default:
            return "unkown type";
    }
}

void xlog_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    errno_t rc = EOK;

    if (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE) {
        CheckPoint *checkpoint = (CheckPoint *)rec;
        CheckPointPlus *ckpt_plus = (CheckPointPlus *)rec;
        CheckPointUndo *ckpt_undo = (CheckPointUndo *)rec;
        time_t time_tmp;
        char ckpttime_str[128];
        const char *strftime_fmt = "%c";

        /* covert checkpoint time to string */
        time_tmp = (time_t)checkpoint->time;
        struct tm *timeStamp = localtime(&time_tmp);
        if (timeStamp == NULL) {
            securec_check(EINVAL_AND_RESET, "\0", "\0");
        } else {
            strftime(ckpttime_str, sizeof(ckpttime_str), strftime_fmt, timeStamp);
        }

        appendStringInfo(buf,
                         "checkpoint: redo %X/%X; "
                         "len %lu; next_csn %lu; recent_global_xmin %lu; "
                         "tli %u; fpw %s; xid " XID_FMT "; oid %u; multi %lu; offset " UINT64_FORMAT
                         "; "
                         "oldest xid " XID_FMT " in DB %u; oldest running xid " XID_FMT "; "
                         "oldest xid with epoch having undo " XID_FMT "; %s at %s; remove_seg %X/%X",
                         (uint32)(checkpoint->redo >> 32), (uint32)checkpoint->redo, ckpt_plus->length,
                         ckpt_plus->next_csn, ckpt_plus->recent_global_xmin, checkpoint->ThisTimeLineID,
                         checkpoint->fullPageWrites ? "true" : "false", checkpoint->nextXid, checkpoint->nextOid,
                         checkpoint->nextMulti, checkpoint->nextMultiOffset, checkpoint->oldestXid,
                         checkpoint->oldestXidDB, checkpoint->oldestActiveXid,
                         (ckpt_plus->length > CKPTPLUSLEN)  ? ckpt_undo->globalRecycleXid : 0,
                         (info == XLOG_CHECKPOINT_SHUTDOWN) ? "shutdown" : "online", ckpttime_str,
                         (uint32)(checkpoint->remove_seg >> 32), (uint32)checkpoint->remove_seg);
    } else if (info == XLOG_NOOP) {
        appendStringInfo(buf, "xlog no-op");
    } else if (info == XLOG_NEXTOID) {
        Oid nextOid;

        rc = memcpy_s(&nextOid, sizeof(Oid), rec, sizeof(Oid));
        securec_check(rc, "\0", "\0");
        appendStringInfo(buf, "nextOid: %u", nextOid);
    } else if (info == XLOG_SWITCH) {
        appendStringInfo(buf, "xlog switch");
    } else if (info == XLOG_RESTORE_POINT) {
        xl_restore_point *xlrec = (xl_restore_point *)rec;

        appendStringInfo(buf, "restore point: %s", xlrec->rp_name);
    } else if (info == XLOG_FPI) {
        /* no further information to print */
    } else if (info == XLOG_FPI_FOR_HINT) {
        appendStringInfo(buf, "page hint");
        if (XLogRecGetDataLen(record) != 0) {
            appendStringInfo(buf, ", type %u", *(uint8 *) XLogRecGetData(record));
        }
    } else if (info == XLOG_BACKUP_END) {
        XLogRecPtr startpoint;

        rc = memcpy_s(&startpoint, sizeof(XLogRecPtr), rec, sizeof(XLogRecPtr));
        securec_check(rc, "\0", "\0");
        appendStringInfo(buf, "backup end: %X/%X", (uint32)(startpoint >> 32), (uint32)startpoint);
    } else if (info == XLOG_PARAMETER_CHANGE) {
        xl_parameter_change xlrec;
        const char *wal_level_str = NULL;
        const struct config_enum_entry *entry = NULL;

        rc = memcpy_s(&xlrec, sizeof(xl_parameter_change), rec, sizeof(xl_parameter_change));
        securec_check(rc, "\0", "\0");

        /* Find a string representation for wal_level */
        wal_level_str = "?";
        for (entry = wal_level_options; entry->name; entry++) {
            if (entry->val == xlrec.wal_level) {
                wal_level_str = entry->name;
                break;
            }
        }

        appendStringInfo(
            buf, "parameter change: max_connections=%d max_prepared_xacts=%d max_locks_per_xact=%d wal_level=%s",
            xlrec.MaxConnections, xlrec.max_prepared_xacts, xlrec.max_locks_per_xact, wal_level_str);
    } else if (info == XLOG_FPW_CHANGE) {
        bool fpw = false;

        rc = memcpy_s(&fpw, sizeof(bool), rec, sizeof(bool));
        securec_check(rc, "\0", "\0");
        appendStringInfo(buf, "full_page_writes: %s", fpw ? "true" : "false");
    } else if (info == XLOG_DELAY_XLOG_RECYCLE) {
        bool delay = false;
        rc = memcpy_s(&delay, sizeof(delay), rec, sizeof(delay));
        securec_check(rc, "\0", "\0");
        appendStringInfo(buf, "delay xlog recycle %s", delay ? "TRUE" : "FALSE");
    } else
        appendStringInfo(buf, "UNKNOWN");
}
