/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * knl_uextremeredo.h
 * the access interfaces of uheap extreme recovery.
 *
 * IDENTIFICATION
 * opengauss_server/src/include/access/ustore/knl_uextremeredo.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UEXTREMEREDO_H
#define KNL_UEXTREMEREDO_H

#include "access/xlogproc.h"
#include "access/ustore/knl_uredo.h"
#include "postgres.h"

#include "miscadmin.h"

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/pg_tablespace.h"

extern XLogRecParseState *UHeapRedoParseToBlock(XLogReaderState *record, uint32 *blocknum);
extern XLogRecParseState *UHeap2RedoParseToBlock(XLogReaderState *record, uint32 *blocknum);
extern XLogRecParseState *UHeapUndoRedoParseToBlock(XLogReaderState *record, uint32 *blocknum);
extern XLogRecParseState *UHeapUndoActionRedoParseToBlock(XLogReaderState *record, uint32 *blocknum);
extern XLogRecParseState *UHeapRollbackFinishRedoParseToBlock(XLogReaderState *record, uint32 *blocknum);

void UHeapRedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo);
void UHeap2RedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo);

void UHeapXlogInsertOperatorPage(RedoBufferInfo *buffer, void *recorddata, bool isinit, bool istoast, void *blkdata,
    Size datalen, TransactionId recxid, Size *freespace);

void RedoUHeapUndoBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo);
void RedoUndoBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo);
void RedoUndoActionBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo);
void RedoRollbackFinishBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo);
#endif
