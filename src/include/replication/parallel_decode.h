/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/include/replication/parallel_decode.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLELDECODE_H
#define PARALLELDECODE_H


#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"

#include "storage/standby.h"

#include "utils/memutils.h"
#include "utils/relfilenodemap.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "replication/parallel_reorderbuffer.h"

#include "catalog/pg_control.h"


typedef enum {
    NOT_DECODE_THREAD,
    DECODE_THREAD_EXIT_NORMAL,
    DECODE_THREAD_EXIT_ABNORMAL,
} DECODEExitStatus;


extern logicalLog* ParallelDecodeChange(ParallelReorderBufferChange* change, ParallelLogicalDecodingContext* ctx,
    ParallelDecodeWorker *worker);
extern void parallel_decode_change_to_json(Relation relation, ParallelReorderBufferChange* change,
    logicalLog *logChange, ParallelLogicalDecodingContext* ctx, int slotId);
extern void parallel_decode_change_to_text(Relation relation, ParallelReorderBufferChange* change,
    logicalLog *logChange, ParallelLogicalDecodingContext* ctx, int slotId);
extern void parallel_decode_change_to_bin(Relation relation, ParallelReorderBufferChange* change,
    logicalLog *logChange, ParallelLogicalDecodingContext* ctx, int slotId);
extern int GetDecodeParallelism(int slotId);
extern ParallelReorderBufferTXN *ParallelReorderBufferGetOldestTXN(ParallelReorderBuffer *rb);
extern logicalLog* GetLogicalLog(ParallelDecodeWorker *worker);
Snapshot GetLocalSnapshot(MemoryContext ctx);

#endif
