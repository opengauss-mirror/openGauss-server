/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

/* -------------------------------------------------------------------------
 *
 * replorigindesc.cpp
 * rmgr descriptor routines for replication/logical/origin.cpp
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/rmgrdesc/replorigindesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/origin.h"

void replorigin_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_REPLORIGIN_SET: {
            xl_replorigin_set *xlrec = (xl_replorigin_set *)rec;

            appendStringInfo(buf, "set %u; lsn %X/%X; force: %d", xlrec->node_id,
                (uint32)(xlrec->remote_lsn >> BITS_PER_INT), (uint32)xlrec->remote_lsn, xlrec->force);
            break;
        }
        case XLOG_REPLORIGIN_DROP: {
            xl_replorigin_drop *xlrec = (xl_replorigin_drop *)rec;

            appendStringInfo(buf, "drop %u", xlrec->node_id);
            break;
        }
        default: {
            break;
        }
    }
}
