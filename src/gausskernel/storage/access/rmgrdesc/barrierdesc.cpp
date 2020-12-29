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

void barrier_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);

    Assert((XLogRecGetInfo(record) & ~XLR_INFO_MASK) == XLOG_BARRIER_CREATE);
    appendStringInfo(buf, "BARRIER %s", rec);
}

