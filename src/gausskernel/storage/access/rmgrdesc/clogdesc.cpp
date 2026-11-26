/* -------------------------------------------------------------------------
 *
 * clogdesc.cpp
 *	  rmgr descriptor routines for access/transam/clog.cpp
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
 *	  src/gausskernel/storage/access/rmgrdesc/clogdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/clog.h"
#ifdef FRONTEND
#include "common/fe_memutils.h"
#endif

const char *clog_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == CLOG_ZEROPAGE) {
        return "clog_zeropage";
    } else if (info == CLOG_TRUNCATE) {
        return "clog_truncate";
    } else {
        return "unkown_type";
    }
}

void clog_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    errno_t rc = EOK;

    if (info == CLOG_ZEROPAGE) {
        int64 pageno = 0;

        /* maybe rec is not int64 alligned */
        rc = memcpy_s(&pageno, sizeof(int64), rec, sizeof(int64));
        securec_check(rc, "", "");
        appendStringInfo(buf, "zeropage: " INT64_FORMAT, pageno);
    } else if (info == CLOG_TRUNCATE) {
        int64 pageno = 0;

        /* maybe rec is not int64 alligned */
        rc = memcpy_s(&pageno, sizeof(int64), rec, sizeof(int64));
        securec_check(rc, "", "");
        appendStringInfo(buf, "truncate before: " INT64_FORMAT, pageno);
    } else
        appendStringInfo(buf, "UNKNOWN");
}
