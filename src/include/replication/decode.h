/* ---------------------------------------------------------------------------------------
 * 
 * decode.h
 *        PostgreSQL WAL to logical transformation
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/decode.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DECODE_H
#define DECODE_H

#include "access/xlogreader.h"
#include "replication/reorderbuffer.h"
#include "replication/logical.h"

void LogicalDecodingProcessRecord(LogicalDecodingContext* ctx, XLogReaderState* record);

#endif
