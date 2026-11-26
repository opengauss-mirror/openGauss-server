/* -------------------------------------------------------------------------
 *
 * relmapdesc.cpp
 *	  rmgr descriptor routines for utils/cache/relmapper.cpp
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
