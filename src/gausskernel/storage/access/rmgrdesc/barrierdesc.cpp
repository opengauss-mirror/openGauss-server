/* -------------------------------------------------------------------------
 *
 * barrierdesc.cpp
 *	  rmgr descriptor routines for backend/pgxc/barrier/barrier.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/barrierdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "pgxc/barrier.h"

const char* barrier_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_BARRIER_CREATE) {
        return "barrier_create";
    } else if (info == XLOG_BARRIER_COMMIT) {
        return "barrier_commit";
    } else if (info == XLOG_BARRIER_SWITCHOVER) {
        return "barrier_switchover";
    } else {
        return "unknow_type";
    }
}


void barrier_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);

    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_BARRIER_CREATE) {
        appendStringInfo(buf, "BARRIER CREATE %s", rec);
    } else if (info == XLOG_BARRIER_COMMIT) {
        appendStringInfo(buf, "BARRIER COMMIT %s", rec);
    } else if (info == XLOG_BARRIER_SWITCHOVER) {
        appendStringInfo(buf, "BARRIER SWITCHOVER %s", rec);
    } else
        appendStringInfo(buf, "UNKNOWN");
}

