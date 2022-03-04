/* -------------------------------------------------------------------------
 *
 * mxactdesc.cpp
 *	  rmgr descriptor routines for access/transam/multixact.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/mxactdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/multixact.h"
#ifdef FRONTEND
#include "common/fe_memutils.h"
#endif

static void OutMember(StringInfo buf, TransactionId xidWithStatus)
{
    appendStringInfo(buf, "" XID_FMT " ", GET_MEMBER_XID_FROM_SLRU_XID(xidWithStatus));
    switch (GET_MEMBER_STATUS_FROM_SLRU_XID(xidWithStatus)) {
        case MultiXactStatusForKeyShare:
            appendStringInfoString(buf, "(keysh) ");
            break;
        case MultiXactStatusForShare:
            appendStringInfoString(buf, "(sh) ");
            break;
        case MultiXactStatusForNoKeyUpdate:
            appendStringInfoString(buf, "(fornokeyupd) ");
            break;
        case MultiXactStatusForUpdate:
            appendStringInfoString(buf, "(forupd) ");
            break;
        case MultiXactStatusNoKeyUpdate:
            appendStringInfoString(buf, "(nokeyupd) ");
            break;
        case MultiXactStatusUpdate:
            appendStringInfoString(buf, "(upd) ");
            break;
        default:
            appendStringInfoString(buf, "(unk) ");
            break;
    }
}

const char* multixact_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    info = info & XLOG_MULTIXACT_MASK;
    if (info == XLOG_MULTIXACT_ZERO_OFF_PAGE) {
        return "muliti_zero_off_page";
    } else if (info == XLOG_MULTIXACT_ZERO_MEM_PAGE) {
        return "muliti_zero_mem_page";
    } else if (info == XLOG_MULTIXACT_CREATE_ID) {
        return "muliti_create_id";
    } else {
        return "unkown_type";
    }
}

void multixact_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    errno_t rc;

    info = info & XLOG_MULTIXACT_MASK;
    if (info == XLOG_MULTIXACT_ZERO_OFF_PAGE) {
        int64 pageno = 0;

        rc = memcpy_s(&pageno, sizeof(int64), rec, sizeof(int64));
        securec_check(rc, "", "");
        appendStringInfo(buf, "zero offsets page: " INT64_FORMAT, pageno);
    } else if (info == XLOG_MULTIXACT_ZERO_MEM_PAGE) {
        int64 pageno = 0;

        rc = memcpy_s(&pageno, sizeof(int64), rec, sizeof(int64));
        securec_check(rc, "", "");
        appendStringInfo(buf, "zero members page: " INT64_FORMAT, pageno);
    } else if (info == XLOG_MULTIXACT_CREATE_ID) {
        xl_multixact_create *xlrec = (xl_multixact_create *)rec;
        int i = 0;

        appendStringInfo(buf, "create multixact " XID_FMT " offset %lu: ", xlrec->mid, xlrec->moff);
        for (i = 0; i < xlrec->nxids; i++)
            OutMember(buf, xlrec->xids[i]);
    } else
        appendStringInfo(buf, "UNKNOWN");
}
