/* ---------------------------------------------------------------------------------------
 * 
 * decode.h
 *        openGauss WAL to logical transformation
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

typedef struct XLogRecordBuffer {
    XLogRecPtr origptr;
    XLogRecPtr endptr;
    XLogReaderState *record;
    char *record_data;
} XLogRecordBuffer;

#include "access/xlogreader.h"
#include "replication/reorderbuffer.h"
#include "replication/logical.h"
extern bool FilterByOrigin(LogicalDecodingContext *ctx, RepOriginId origin_id);
extern bool FilterByOrigin(ParallelLogicalDecodingContext *ctx, RepOriginId origin_id);

void LogicalDecodingProcessRecord(LogicalDecodingContext* ctx, XLogReaderState* record);
void AreaLogicalDecodingProcessRecord(LogicalDecodingContext* ctx, XLogReaderState* record);

extern Pointer UGetMultiInsertXlrec(XLogReaderState *record, CommitSeqNo* curCSN);

void DecodeXLogTuple(const char *data, Size len, ReorderBufferTupleBuf *tuple, bool isHeapTuple);
extern Pointer UGetXlrec(XLogReaderState * record);
size_t DecodeUndoMeta(const char* data);
bool FilterRecord(LogicalDecodingContext *ctx, XLogReaderState *r, uint8 flags, RelFileNode* rnode);
void UpdateUndoBody(Size* addLenPtr, char* data, uint8 flag, uint32* toastLen);
char *UpdateOldTupleCalc(bool isInplaceUpdate, XLogReaderState *r, char **tupleOld, Size *tuplelenOld,
    uint32* toastLen);
void DecodeUHeapToastTuple(const char * toastData, Size len, ReorderBufferTupleBuf *tuple);
extern void logicalddl_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
extern void ParallelDecodeWorkerMain(void* point);
extern void LogicalReadWorkerMain(void* point);
#endif
