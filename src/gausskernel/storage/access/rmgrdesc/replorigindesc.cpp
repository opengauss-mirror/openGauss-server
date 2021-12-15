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
