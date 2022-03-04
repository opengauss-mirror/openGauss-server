/* -------------------------------------------------------------------------
 *
 * relmapdesc.cpp
 *	  rmgr descriptor routines for utils/cache/relmapper.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/relmapdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/relmapper.h"

const char* relmap_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_RELMAP_UPDATE) {
        return "relmap_update";
    } else {
        return "unkown_type";
    }
}

void relmap_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_RELMAP_UPDATE) {
        xl_relmap_update *xlrec = (xl_relmap_update *)rec;

        appendStringInfo(buf, "update relmap: database %u tablespace %u size %d", xlrec->dbid, xlrec->tsid,
                         xlrec->nbytes);
    } else
        appendStringInfo(buf, "UNKNOWN");
}
