/*-------------------------------------------------------------------------
 *
 * genericdesc.cpp
 *    rmgr descriptor routines for access/transam/generic_xlog.cpp
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
