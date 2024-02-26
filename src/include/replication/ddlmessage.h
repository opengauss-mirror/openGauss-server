/*-------------------------------------------------------------------------
 * ddlmessage.h
 * 
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/include/replication/ddlmessage.h
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_DDL_MESSAGE_H
#define PG_LOGICAL_DDL_MESSAGE_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "nodes/nodes.h"


/*
 * Support for keeping track of deparsed commands.
 */
typedef enum DeparsedCommandType
{
    DCT_ObjectCreate,
    DCT_ObjectDrop,
    DCT_SimpleCmd,
    DCT_TableDropEnd,
    DCT_TableDropStart
} DeparsedCommandType;

/*
 * Generic logical decoding DDL message wal record.
 */
typedef struct xl_logical_ddl_message
{
    Oid         dbId;           /* database Oid emitted from */
    Size        prefix_size;    /* length of prefix including null terminator */
    Oid         relid;          /* id of the table */
    DeparsedCommandType cmdtype;    /* type of sql command */
    Size        message_size;   /* size of the message */

    /*
     * payload, including null-terminated prefix of length prefix_size
     */
    char        message[FLEXIBLE_ARRAY_MEMBER];
} xl_logical_ddl_message;

#define SizeOfLogicalDDLMessage (offsetof(xl_logical_ddl_message, message))

extern XLogRecPtr LogLogicalDDLMessage(const char *prefix, Oid relid, DeparsedCommandType cmdtype,
                                       const char *ddl_message, size_t size);

/* RMGR API*/
#define XLOG_LOGICAL_DDL_MESSAGE    0x00
void logicalddlmsg_redo(XLogReaderState *record);
void logicalddlmsg_desc(StringInfo buf, XLogReaderState *record);
const char *logicalddlmsg_identify(uint8 info);
const char *logicalddlmsg_type_name(uint8 subtype);

#endif
