/* -------------------------------------------------------------------------
 *
 * knl_uam.h
 *  header file for postgres multi-version btree access method implementation.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_uam.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef KNL_UAM_H
#define KNL_UAM_H

#include "nodes/execnodes.h"
#include "access/ustore/knl_uheap.h"

bool UHeapFetchRowVersion(TidScanState *node, Relation relation, ItemPointer tid, Snapshot snapshot,
    TupleTableSlot *slot);

#endif