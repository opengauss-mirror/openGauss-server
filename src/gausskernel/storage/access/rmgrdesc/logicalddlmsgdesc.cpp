/*-------------------------------------------------------------------------
 *
 * logicalddlmsgdesc.cpp
 *    rmgr descriptor routines for replication/logical/ddlmessage.c
 *
 * Portions Copyright (c) 2015-2023, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/rmgrdesc/logicalddlmsgdesc.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/ddlmessage.h"

void
logicalddlmsg_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info == XLOG_LOGICAL_DDL_MESSAGE) {
        int cnt = 0;
        xl_logical_ddl_message *xlrec = (xl_logical_ddl_message *) rec;
        char *prefix = xlrec->message;
        char *message = xlrec->message + xlrec->prefix_size;

        Assert(prefix[xlrec->prefix_size - 1] == '\0');

        appendStringInfo(buf, "prefix \"%s\"; payload (%zu bytes): ",
                         prefix, xlrec->message_size);
        appendStringInfo(buf, "relid %u cmdtype %u", xlrec->relid, xlrec->cmdtype);
        /* Write message payload as a series of hex bytes */
        for (; cnt < xlrec->message_size - 1; cnt++) {
            appendStringInfo(buf, "%02X ", (unsigned char) message[cnt]);
        }
        appendStringInfo(buf, "%02X", (unsigned char) message[cnt]);
    }
}

const char *
logicalddlmsg_identify(uint8 info)
{
    if ((info & ~XLR_INFO_MASK) == XLOG_LOGICAL_DDL_MESSAGE)
        return "DDL";

    return NULL;
}

const char *
logicalddlmsg_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_LOGICAL_DDL_MESSAGE) {
        return "logical_ddl_message";
    } else {
        return "unknown_type";
    }
}