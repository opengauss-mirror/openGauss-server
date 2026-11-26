/* -------------------------------------------------------------------------
 *
 * gistdesc.cpp
 *	  rmgr descriptor routines for access/gist/gistxlog.cpp
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
#include "storage/smgr/relfilenode.h"

static void out_gistxlogPageSplit(StringInfo buf, gistxlogPageSplit *xlrec)
{
    appendStringInfo(buf, "page_split: splits to %hu pages", xlrec->npage);
}

const char* gist_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    switch (info) {
        case XLOG_GIST_PAGE_UPDATE:
            return "gist_page_update";
            break;
        case XLOG_GIST_PAGE_SPLIT:
            return "gist_page_split";
            break;
        case XLOG_GIST_CREATE_INDEX:
            return "gist_create_index";
            break;
        default:
            break;
    }
    return "unknow_type";
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
