/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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
 * -------------------------------------------------------------------------
 *
 * vector_storage.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/vector_storage.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VECTOR_STORAGE_H
#define VECTOR_STORAGE_H

#include "postgres.h"

#include "access/datavec/vector_buffer.h"
#include "access/itup.h"
#include "utils/rel.h"

#ifdef __cplusplus
extern "C" {
#endif

/* HNSW / IVFFlat / DiskANN all keep the AM metapage at block 0. */
#define VEC_PAYLOAD_METAPAGE_BLKNO 0

#define VEC_PAYLOAD_STATUS_LIVE 1
#define VEC_PAYLOAD_STATUS_DEAD 2
#define VEC_PAYLOAD_STATUS_REUSABLE 3

typedef struct VecPayloadTupleHeaderData {
    uint16 status;
    uint8 kind;
    uint8 flags;
    uint32 payloadLen;
} VecPayloadTupleHeaderData;

static inline Size VEC_PAYLOAD_TUPLE_SIZE(Size len)
{
    return MAXALIGN(sizeof(VecPayloadTupleHeaderData) + len);
}

static inline char *VecPayloadTupleGetData(VecPayloadTupleHeaderData *tup)
{
    return (char *)tup + sizeof(VecPayloadTupleHeaderData);
}

static inline const char *VecPayloadTupleGetConstData(const VecPayloadTupleHeaderData *tup)
{
    return (const char *)tup + sizeof(VecPayloadTupleHeaderData);
}

/*
 * On-disk payload slot reference shared by HNSW / IVFFlat / DiskANN.
 * Layout must stay stable; this is not an HNSW-only type.
 */
typedef struct VecPayloadDiskRef {
    ItemPointerData tid;
    uint32 payloadLen;
    uint8 kind;
    uint8 reserved[3];
} VecPayloadDiskRef;

typedef struct VecPayloadPin {
    VectorBufferHandle handle;
    VectorBufferAccess *ownedAccess;
    Datum datum;
} VecPayloadPin;

typedef struct VecPayloadPinRequest {
    VecPayloadKind kind;
    uint32 expectedPayloadLen;
    VectorBufferAccess *access;
} VecPayloadPinRequest;

typedef struct VecPayloadBuildState {
    Relation index;
    ForkNumber forkNum;
    Buffer buf;
    Page page;
    BlockNumber insertBlkno;
} VecPayloadBuildState;

typedef struct VecPayloadInput {
    VecPayloadKind kind;
    const void *data;
    uint32 len;
} VecPayloadInput;

typedef struct VecPayloadInsertRequest {
    ForkNumber forkNum;
    BlockNumber startBlkno;
    VecPayloadDiskRef *outRef;
    BlockNumber *insertBlknoOut;
    /* Optional parallel-build P_NEW mutex; runtime callers pass NULL. */
    LWLock *buildExtensionLock;
} VecPayloadInsertRequest;

typedef struct VecPayloadInsertIndexTupleRequest {
    ForkNumber forkNum;
    BlockNumber startBlkno;
    ItemPointer heapTid;
    BlockNumber *insertBlknoOut;
    LWLock *buildExtensionLock;
} VecPayloadInsertIndexTupleRequest;

IndexTuple VecPayloadFormIndexTupleFromRef(
    Relation index, const VecPayloadDiskRef *rawRef, ItemPointer heapTid);
bool VecPayloadIndexTupleGetRef(IndexTuple itup, Relation index, VecPayloadDiskRef *outRef);
IndexTuple VecPayloadInsertIndexTuple(Relation index, const VecPayloadInput *payload,
    const VecPayloadInsertIndexTupleRequest *request);

void VecPayloadBeginBuild(VecPayloadBuildState *state, Relation index, ForkNumber forkNum);
void VecPayloadPutBuild(VecPayloadBuildState *state, const VecPayloadInput *payload, VecPayloadDiskRef *outRef);
void VecPayloadFlushBuild(VecPayloadBuildState *state);
void VecPayloadEndBuild(VecPayloadBuildState *state, BlockNumber *payloadInsertBlkno);

/*
 * Runtime (non-build) payload writes. Each page change is wrapped in GenericXLog so it
 * replays on a standby on its own; the build path still relies on LogNewpageRange.
 *
 * startBlkno is the caller's insert hint. HNSW / IVFFlat / DiskANN all keep
 * payloadInsertBlkno and payloadFreeHead on the AM metapage (block 0). Insert
 * reuses a free slot when one exists (invalidate, then overwrite); otherwise
 * it appends. insertBlknoOut reports the landing block. The metapage hint and
 * livePayloads are updated in the same WAL record; callers must not bump
 * livePayloads again.
 */
void VecPayloadInsert(Relation index, const VecPayloadInput *payload, const VecPayloadInsertRequest *request);
bool VecPayloadPinGet(Relation index, const VecPayloadDiskRef *diskRef,
    const VecPayloadPinRequest *request, VecPayloadPin *pin);
void VecPayloadUnpin(VecPayloadPin *pin);
/* Recycle a retired graph/list tuple's payload and clear its reference in one WAL record. */
void VecPayloadRecycle(Relation index, ForkNumber forkNum, const ItemPointerData *indexTid);
bool VecPayloadRedoNeedsOldImage(Page page);
void VecPayloadRedoInvalidateIfNeeded(const RelFileNode *rnode, BlockNumber blkno, Page newPage);
void VecPayloadRedoInvalidateChangedSlots(const RelFileNode *rnode, BlockNumber blkno, Page oldPage, Page newPage);

#ifdef __cplusplus
}
#endif

#endif /* VECTOR_STORAGE_H */
