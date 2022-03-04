/* -------------------------------------------------------------------------
 *
 * knl_uhio.h
 * the I/O interfaces of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_uhio.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UIO_H
#define KNL_UIO_H

#include "access/hio.h"

/* "options" flag bits for heap_insert */
#define UHEAP_INSERT_SKIP_WAL HEAP_INSERT_SKIP_WAL
#define UHEAP_INSERT_SKIP_FSM HEAP_INSERT_SKIP_FSM
#define UHEAP_INSERT_FROZEN HEAP_INSERT_FROZEN
#define UHEAP_INSERT_EXTEND 0x0020

#define GET_BUF_FOR_UTUPLE_LOOP_LIMIT 2

extern BlockNumber RelationPruneOptional(Relation relation, Size requiredSize);
extern BlockNumber RelationPruneBlockAndReturn(Relation relation, BlockNumber start_block,
    BlockNumber max_blocks_to_scan, Size required_size, BlockNumber *next_block);

extern Buffer RelationGetBufferForUTuple(Relation relation, Size len, Buffer otherBuffer, int options,
    BulkInsertState bistate);

#endif
