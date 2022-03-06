/* -------------------------------------------------------------------------
 *
 * clogdesc.cpp
 *	  rmgr descriptor routines for access/transam/clog.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
