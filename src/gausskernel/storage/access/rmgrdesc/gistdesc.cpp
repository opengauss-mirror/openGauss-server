/* -------------------------------------------------------------------------
 *
 * gistdesc.cpp
 *	  rmgr descriptor routines for access/gist/gistxlog.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/gistdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gist_private.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"

static void out_gistxlogPageSplit(StringInfo buf, gistxlogPageSplit *xlrec)
{
    appendStringInfo(buf, "page_split: splits to %hu pages", xlrec->npage);
}

void gist_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_GIST_PAGE_UPDATE:
            appendStringInfo(buf, "page_update: ");
            break;
        case XLOG_GIST_PAGE_SPLIT:
            out_gistxlogPageSplit(buf, (gistxlogPageSplit *)rec);
            break;
        case XLOG_GIST_CREATE_INDEX:
            appendStringInfo(buf, "create_index: ");
            break;
        default:
            appendStringInfo(buf, "unknown gist op code %hhu", info);
            break;
    }
}
