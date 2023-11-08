/*-------------------------------------------------------------------------
 *
 * ddlmessage.cpp
 *    Logical DDL messages.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/replication/logical/ddlmessage.cpp
 *
 * NOTES
 *
 * Logical DDL messages allow XLOG logging of DDL command strings that
 * get passed to the logical decoding plugin. In normal XLOG processing they
 * are same as NOOP.
 *
 * Unlike generic logical messages, these DDL messages have only transactional
 * mode. Note by default DDLs in PostgreSQL are transactional.
 *
 * These messages are part of current transaction and will be sent to
 * decoding plugin similar to DML operations.
 *
 * Every message includes a prefix to avoid conflicts between different decoding
 * plugins. Plugin authors must take special care to use a unique prefix (e.g one
 * idea is to include the name of the extension).
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "replication/logical.h"
#include "replication/ddlmessage.h"
#include "tcop/ddldeparse.h"
#include "utils/memutils.h"

/*
 * Write logical decoding DDL message into XLog.
 */
XLogRecPtr
LogLogicalDDLMessage(const char *prefix, Oid relid, DeparsedCommandType cmdtype,
                     const char *message, size_t size)
{
    xl_logical_ddl_message xlrec;

     /* Ensure we have a valid transaction id. */
    Assert(IsTransactionState());
    GetCurrentTransactionId();

    elog(LOG, "LogLogicalDDLMessage : %s", message);
    char *tmp = pstrdup(message);
    char *owner = NULL;

    if (cmdtype != DCT_TableDropStart) {
        char *decodestring = deparse_ddl_json_to_string(tmp, &owner);
        elog(LOG, "will decode to : %s, [owner %s]", decodestring, owner ? owner : "none");
    }
    pfree(tmp);

    xlrec.dbId = u_sess->proc_cxt.MyDatabaseId;
    /* Trailing zero is critical; see logicalddlmsg_desc */
    xlrec.prefix_size = strlen(prefix) + 1;
    xlrec.message_size = size;
    xlrec.relid = relid;
    xlrec.cmdtype = cmdtype;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, SizeOfLogicalDDLMessage);
    XLogRegisterData((char *) prefix, xlrec.prefix_size);
    XLogRegisterData((char *) message, size);

    return XLogInsert(RM_LOGICALDDLMSG_ID, XLOG_LOGICAL_DDL_MESSAGE);
}

/*
 * Redo is basically just noop for logical decoding DDL messages.
 */
void
logicalddlmsg_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info != XLOG_LOGICAL_DDL_MESSAGE)
        elog(PANIC, "logicalddlmsg_redo: unknown op code %u", info);

    /* This is only interesting for logical decoding, see decode.c. */
}
