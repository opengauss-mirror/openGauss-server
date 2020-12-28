/* -------------------------------------------------------------------------
 *
 * seqdesc.cpp
 *	  rmgr descriptor routines for commands/sequence.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/seqdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/sequence.h"

void seq_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    xl_seq_rec *xlrec = (xl_seq_rec *)rec;

    if (info == XLOG_SEQ_LOG)
        appendStringInfo(buf, "log: ");
    else {
        appendStringInfo(buf, "UNKNOWN");
        return;
    }

    appendStringInfo(buf, "rel %u/%u/%u", xlrec->node.spcNode, xlrec->node.dbNode, xlrec->node.relNode);
}
