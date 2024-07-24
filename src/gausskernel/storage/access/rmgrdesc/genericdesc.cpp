/*-------------------------------------------------------------------------
 *
 * genericdesc.cpp
 *    rmgr descriptor routines for access/transam/generic_xlog.cpp
 *
 * Portions Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/storage/access/rmgrdesc/genericdesc.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "lib/stringinfo.h"
#include "storage/smgr/relfilenode.h"

/*
 * Description of generic xlog record: write page regions that this record
 * overrides.
 */
void
generic_desc(StringInfo buf, XLogReaderState *record)
{
    errno_t ret = EOK;
    Pointer ptr = XLogRecGetData(record),
        end = ptr + XLogRecGetDataLen(record);

    while (ptr < end) {
        OffsetNumber offset, length;

        ret = memcpy_s(&offset, sizeof(offset), ptr, sizeof(offset));
        securec_check(ret, "\0", "\0");
        ptr += sizeof(offset);
        ret = memcpy_s(&length, sizeof(length), ptr, sizeof(length));
        securec_check(ret, "\0", "\0");
        ptr += sizeof(length);
        ptr += length;

        if (ptr < end)
            appendStringInfo(buf, "offset %u, length %u; ", offset, length);
        else
            appendStringInfo(buf, "offset %u, length %u", offset, length);
    }

    return;
}

/*
 * Identification of generic xlog record: we don't distinguish any subtypes
 * inside generic xlog records.
 */
const char *
generic_identify(uint8 info)
{
    return "Generic";
}
