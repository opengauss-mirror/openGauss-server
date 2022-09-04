/* ---------------------------------------------------------------------------------------
 *
 * logical_parse.h
 *        openGauss parallel decoding parse xlog.
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        src/include/replication/logical_parse.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LOGICAL_PARSE_H
#define LOGICAL_PARSE_H
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"

#include "storage/standby.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"

#include "utils/memutils.h"
#include "utils/relfilenodemap.h"
#include "utils/atomic.h"
#include "cjson/cJSON.h"

#include "catalog/pg_control.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "replication/parallel_decode.h"
#include "replication/parallel_reorderbuffer.h"

extern logicalLog* ParallelDecodeChange(ParallelReorderBufferChange* change, ParallelLogicalDecodingContext* ctx,
    ParallelDecodeWorker *worker);
extern void parallel_decode_change_to_json(Relation relation, ParallelReorderBufferChange* change,
    logicalLog *logChange, ParallelLogicalDecodingContext* ctx, int slotId);
extern void parallel_decode_change_to_text(Relation relation, ParallelReorderBufferChange* change,
    logicalLog *logChange, ParallelLogicalDecodingContext* ctx, int slotId);
extern void ParseProcessRecord(ParallelLogicalDecodingContext *ctx, XLogReaderState *record,
    ParallelDecodeReaderWorker *worker);
extern void ParseHeapOp(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
extern void ParseHeap2Op(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf,
    ParallelDecodeReaderWorker *worker);
extern void ParseAbortXlog(ParallelLogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
    TransactionId *sub_xids, int nsubxacts, ParallelDecodeReaderWorker *worker);
extern void ParseInsertXlog(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf,
    ParallelDecodeReaderWorker *worker);
extern int GetDecodeParallelism(int slotId);
extern int GetParallelQueueSize(int slotId);
extern ParallelReorderBufferTXN *ParallelReorderBufferGetOldestTXN(ParallelReorderBuffer *rb);
extern logicalLog* GetLogicalLog(ParallelDecodeWorker *worker, int slotId = -1);
extern void PutChangeQueue(int slotId, ParallelReorderBufferChange *change);
extern bool CheckToastTuple(ParallelReorderBufferChange *change, ParallelLogicalDecodingContext *ctx,
    Relation relation, bool istoast, int slotId);
extern bool ToastTupleReplace(ParallelReorderBuffer *rb, Relation relation, ParallelReorderBufferChange *change,
    Oid partationReltoastrelid, bool isHeap, bool freeNow, int slotId);
extern void ToastTupleAppendChunk(ParallelReorderBuffer *rb, Relation relation,
    ParallelReorderBufferChange *change, int slotId);
#endif
