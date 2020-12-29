/**
 * @file cma_xlog.h
 * @brief Define functions of reading xlog for agent.
 * @author xxx
 * @version 1.0
 * @date 2020-08-03
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */


#ifndef CMA_XLOG_H
#define CMA_XLOG_H

#include "access/xlog_basic.h"

typedef struct AgentXLogPageReadPrivate {
    const char* datadir;
    TimeLineID tli;
} AgentXLogPageReadPrivate;

typedef struct AgentXlogFileList {
    AgentXlogFileList* next;
    char* filename;
} AgentXlogFileList;

void* DNXlogCheckMain(void * const arg);

/*
 * Allocate and initialize a new XLogReader.
 *
 * Returns NULL if the xlogreader couldn't be allocated.
 */
extern XLogReaderState* AgentXLogReaderAllocate(XLogPageReadCB pagereadfunc, void* private_data);

/*
 * Find the first record with at an lsn >= RecPtr.
 *
 * Useful for checking wether RecPtr is a valid xlog address for reading and to
 * find the first valid address after some address when dumping records for
 * debugging purposes.
 */
extern XLogRecPtr AgentXLogFindNextRecord(XLogReaderState* state, XLogRecPtr RecPtr);

/*
 * Attempt to read an XLOG record.
 *
 * If RecPtr is not NULL, try to read a record at that position.  Otherwise
 * try to read a record just after the last one previously read.
 *
 * If the page read callback fails to read the requested data, NULL is
 * returned.  The callback is expected to have reported the error; errormsg
 * is set to NULL.
 *
 * If the reading fails for some other reason, NULL is also returned, and
 * *errormsg is set to a string with details of the failure.
 *
 * The returned pointer (or *errormsg) points to an internal buffer that's
 * valid until the next call to XLogReadRecord.
 */
extern XLogRecord* AgentXLogReadRecord(
    XLogReaderState* state, XLogRecPtr RecPtr, char** errormsg, bool readoldversion = false);

/*
 * Free the xlog reader memory.
 */
extern void AgentXLogReaderFree(XLogReaderState* state);

#endif