/* -------------------------------------------------------------------------
 *
 * tblspcdesc.cpp
 *	  rmgr descriptor routines for commands/tablespace.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/tblspcdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/tablespace.h"

const char* tblspc_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_TBLSPC_CREATE) {
        return "tblspc_create";
    } else if (info == XLOG_TBLSPC_RELATIVE_CREATE) {
        return "tblspc_relative_create";
    } else if (info == XLOG_TBLSPC_DROP) {
        return "tblspc_drop";
    } else {
        return "unkown_type";
    }
}

void tblspc_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_TBLSPC_CREATE) {
        xl_tblspc_create_rec *xlrec = (xl_tblspc_create_rec *)rec;

        appendStringInfo(buf, "create tablespace: %u \"%s\"", xlrec->ts_id, xlrec->ts_path);
    } else if (info == XLOG_TBLSPC_RELATIVE_CREATE) {
        xl_tblspc_create_rec *xlrec = (xl_tblspc_create_rec *)rec;
        appendStringInfo(buf, "create tablespace(relative location): %u \"%s\"", xlrec->ts_id, xlrec->ts_path);
    } else if (info == XLOG_TBLSPC_DROP) {
        xl_tblspc_drop_rec *xlrec = (xl_tblspc_drop_rec *)rec;

        appendStringInfo(buf, "drop tablespace: %u", xlrec->ts_id);
    } else
        appendStringInfo(buf, "UNKNOWN");
}
