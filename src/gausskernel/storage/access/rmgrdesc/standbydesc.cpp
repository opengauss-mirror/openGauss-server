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
#include "storage/sinval.h"

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

/*
 * This routine is used by both standby_desc and xact_desc, because
 * transaction commits and XLOG_INVALIDATIONS messages contain invalidations;
 * it seems pointless to duplicate the code.
 */
void standby_desc_invalidations(StringInfo buf, int nmsgs, SharedInvalidationMessage *msgs,
                                Oid dbId, Oid tsId, bool relcacheInitFileInval)
{
    int i;

    /* Do nothing if there are no invalidation messages */
    if (nmsgs <= 0) {
        return;
    }

    if (relcacheInitFileInval)
        appendStringInfo(buf, "; relcache init file inval dbid %u tsid %u",
                         dbId, tsId);

    appendStringInfoString(buf, "; inval msgs:");
    for (i = 0; i < nmsgs; i++) {
        SharedInvalidationMessage *msg = &msgs[i];

        if (msg->id >= 0)
            appendStringInfo(buf, " catcache %d", msg->id);
        else if (msg->id == SHAREDINVALCATALOG_ID)
            appendStringInfo(buf, " catalog %u", msg->cat.catId);
        else if (msg->id == SHAREDINVALRELCACHE_ID)
            appendStringInfo(buf, " relcache %u", msg->rc.relId);
        /* not expected, but print something anyway */
        else if (msg->id == SHAREDINVALSMGR_ID)
            appendStringInfoString(buf, " smgr");
        /* not expected, but print something anyway */
        else if (msg->id == SHAREDINVALRELMAP_ID)
            appendStringInfo(buf, " relmap db %u", msg->rm.dbId);
        else
            appendStringInfo(buf, " unrecognized id %d", msg->id);
    }
}
