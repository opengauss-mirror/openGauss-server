/* -------------------------------------------------------------------------
 *
 * standbydesc.cpp
 *	  rmgr descriptor routines for storage/ipc/standby.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/standbydesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/standby.h"

const char* standby_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_STANDBY_LOCK) {
        return "standby_lock";
    } else if (info == XLOG_RUNNING_XACTS) {
        return "running_xact";
    } else if (info == XLOG_STANDBY_CSN) {
        return "standby_csn";
    } else if (info == XLOG_STANDBY_UNLOCK) {
        return "standby_unlock";
#ifndef ENABLE_MULTIPLE_NODES
    } else if (info == XLOG_STANDBY_CSN_COMMITTING) {
        return "standby_csn_committing";
    } else if (info == XLOG_STANDBY_CSN_ABORTED) {
        return "standby_csn_abort";
#endif
    } else {
        return "unkown_type";
    }
}

void standby_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_STANDBY_LOCK) {
        if ((XLogRecGetInfo(record) & PARTITION_ACCESS_EXCLUSIVE_LOCK_UPGRADE_FLAG) == 0) {
            xl_standby_locks *xlrec = (xl_standby_locks *)rec;
            appendStringInfo(buf, "AccessExclusive locks: nlocks %d ", xlrec->nlocks);
            for (int i = 0; i < xlrec->nlocks; i++) {
                appendStringInfo(buf, " xid " XID_FMT " db %u rel %u seq %u", xlrec->locks[i].xid,
                                 xlrec->locks[i].dbOid, xlrec->locks[i].relOid, InvalidOid);
            }
        } else {
            XLogStandbyLocksNew *xlrec = (XLogStandbyLocksNew *)rec;
            appendStringInfo(buf, "AccessExclusive locks: nlocks %d ", xlrec->nlocks);
            for (int i = 0; i < xlrec->nlocks; i++) {
                appendStringInfo(buf, " xid " XID_FMT " db %u rel %u seq %u", xlrec->locks[i].xid,
                                 xlrec->locks[i].dbOid, xlrec->locks[i].relOid, xlrec->locks[i].seq);
            }
        }
    } else if (info == XLOG_RUNNING_XACTS) {
        appendStringInfo(buf, " XLOG_RUNNING_XACTS");
    } else if (info == XLOG_STANDBY_CSN) {
        appendStringInfo(buf, " XLOG_STANDBY_CSN");
    } else if (info == XLOG_STANDBY_UNLOCK) {
        if ((XLogRecGetInfo(record) & PARTITION_ACCESS_EXCLUSIVE_LOCK_UPGRADE_FLAG) == 0) {
            xl_standby_locks *xlrec = (xl_standby_locks *)rec;
            appendStringInfo(buf, "AccessExclusive locks: nlocks %d ", xlrec->nlocks);
            for (int i = 0; i < xlrec->nlocks; i++) {
                appendStringInfo(buf, " xid " XID_FMT " db %u rel %u seq %u", xlrec->locks[i].xid,
                                 xlrec->locks[i].dbOid, xlrec->locks[i].relOid, InvalidOid);
            }
        } else {
            XLogStandbyLocksNew *xlrec = (XLogStandbyLocksNew *)rec;
            appendStringInfo(buf, "AccessExclusive locks: nlocks %d ", xlrec->nlocks);
            for (int i = 0; i < xlrec->nlocks; i++) {
                appendStringInfo(buf, " xid " XID_FMT " db %u rel %u seq %u", xlrec->locks[i].xid,
                                 xlrec->locks[i].dbOid, xlrec->locks[i].relOid, xlrec->locks[i].seq);
            }
        }
    } else if (info == XLOG_STANDBY_CSN_COMMITTING) {
        uint64 *id = ((uint64 *)XLogRecGetData(record));
        appendStringInfo(buf, " XLOG_STANDBY_CSN_COMMITTING, xid %lu, csn %lu", id[0], id[1]);
    } else if (info == XLOG_STANDBY_CSN_ABORTED) {
        uint64 *id = ((uint64 *)XLogRecGetData(record));
        appendStringInfo(buf, " XLOG_STANDBY_CSN_ABORTED, xid %lu", id[0]);
    } else
        appendStringInfo(buf, "UNKNOWN");
}
