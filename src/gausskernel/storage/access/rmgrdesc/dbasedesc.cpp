/* -------------------------------------------------------------------------
 *
 * dbasedesc.cpp
 *	  rmgr descriptor routines for commands/dbcommands.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/dbasedesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/dbcommands.h"
#include "lib/stringinfo.h"

const char* dbase_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_DBASE_CREATE) {
        return "db_create";
    } else if (info == XLOG_DBASE_DROP) {
        return "db_drop";
    } else {
        return "unkown_type";
    }
}

void dbase_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_DBASE_CREATE) {
        xl_dbase_create_rec *xlrec = (xl_dbase_create_rec *)rec;

        appendStringInfo(buf, "create db: copy dir %u/%u to %u/%u", xlrec->src_db_id, xlrec->src_tablespace_id,
                         xlrec->db_id, xlrec->tablespace_id);
    } else if (info == XLOG_DBASE_DROP) {
        xl_dbase_drop_rec *xlrec = (xl_dbase_drop_rec *)rec;

        appendStringInfo(buf, "drop db: dir %u/%u", xlrec->db_id, xlrec->tablespace_id);
    } else
        appendStringInfo(buf, "UNKNOWN");
}
