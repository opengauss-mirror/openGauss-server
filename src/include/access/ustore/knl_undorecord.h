/* -------------------------------------------------------------------------
 *
 * knl_undorecord.h
 * the xact access interfaces of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * opengauss_server/src/include/access/ustore/knl_undorecord.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UNDORECORD_H
#define KNL_UNDORECORD_H

#include "access/ustore/knl_utype.h"

typedef struct UndoRecordData {
    RmgrId rmid;
    uint16 size;
    UndoRecordAddress prev;
    uint8 data[FLEXIBLE_ARRAY_MEMBER];
} UndoRecordData;

typedef UndoRecordData *UndoRecord;

#endif
