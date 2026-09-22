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
 * hnsw_vector_storage.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnsw_vector_storage.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/datavec/hnsw.h"
#include "access/datavec/vector_storage.h"
#include "securec.h"
#include "utils/rel.h"
#include "access/datavec/hnsw_vector_storage.h"

static bool HnswPayloadDiskRefReservedIsZero(const VecPayloadDiskRef *diskRef)
{
    for (size_t i = 0; i < lengthof(diskRef->reserved); i++) {
        if (diskRef->reserved[i] != 0) {
            return false;
        }
    }
    return true;
}

static bool HnswVectorStoragePayloadLenIsValid(uint32 payloadLen)
{
    Size rawSize;
    Size alignedSize;

    if (payloadLen < VARHDRSZ || (Size)payloadLen > (Size)HNSW_MAX_SIZE) {
        return false;
    }

    rawSize = sizeof(VecPayloadTupleHeaderData) + (Size)payloadLen;
    alignedSize = MAXALIGN(rawSize);
    return alignedSize >= rawSize && alignedSize <= (Size)HNSW_MAX_SIZE;
}

static bool HnswDecodeVectorStorageMeta(
    HnswMetaPage metap, uint32 *payloadLen, BlockNumber *graphHeadBlkno)
{
    uint32 decodedPayloadLen;
    BlockNumber decodedGraphHeadBlkno;

    if (metap == NULL || metap->version != HNSW_VECTOR_STORAGE_VERSION ||
        metap->flags != HNSW_META_HAS_VECTOR_STORAGE ||
        metap->payloadFormatVersion != HNSW_PAYLOAD_FORMAT_VERSION ||
        metap->reservedFlags != 0) {
        return false;
    }

    decodedPayloadLen = metap->payloadLen;
    decodedGraphHeadBlkno = metap->graphHeadBlkno;
    if (!HnswVectorStoragePayloadLenIsValid(decodedPayloadLen)) {
        return false;
    }

    if (payloadLen != NULL) {
        *payloadLen = decodedPayloadLen;
    }
    if (graphHeadBlkno != NULL) {
        *graphHeadBlkno = decodedGraphHeadBlkno;
    }
    return true;
}

HnswVectorStorageMetaLayout HnswClassifyVectorStorageMeta(HnswMetaPage metap, uint32 *payloadLen)
{
    if (payloadLen != NULL) {
        *payloadLen = 0;
    }
    if (metap == NULL) {
        return HNSW_VECTOR_STORAGE_META_INVALID;
    }

    if (metap->version == HNSW_VERSION) {
        if (metap->flags == 0 && metap->payloadFormatVersion == 0 &&
            metap->reservedFlags == 0 && metap->payloadLen == 0) {
            return HNSW_VECTOR_STORAGE_META_LEGACY;
        }
        return HNSW_VECTOR_STORAGE_META_INVALID;
    }

    if (!HnswDecodeVectorStorageMeta(metap, payloadLen, NULL)) {
        return HNSW_VECTOR_STORAGE_META_INVALID;
    }
    return HNSW_VECTOR_STORAGE_META_V2;
}

bool HnswGetVectorStorageGraphHeadBlkno(HnswMetaPage metap, BlockNumber *graphHeadBlkno)
{
    BlockNumber decodedGraphHeadBlkno;

    if (graphHeadBlkno == NULL ||
        !HnswDecodeVectorStorageMeta(metap, NULL, &decodedGraphHeadBlkno) ||
        !BlockNumberIsValid(decodedGraphHeadBlkno)) {
        return false;
    }

    *graphHeadBlkno = decodedGraphHeadBlkno;
    return true;
}

bool HnswElementTupleMatchesMetaLayout(HnswElementTuple etup, HnswVectorStorageMetaLayout layout)
{
    if (etup == NULL || !HnswIsElementTuple(etup)) {
        return false;
    }
    if (layout == HNSW_VECTOR_STORAGE_META_LEGACY) {
        return !HnswElementTupleIsVectorStorage(etup);
    }
    if (layout == HNSW_VECTOR_STORAGE_META_V2) {
        return HnswElementTupleIsVectorStorage(etup);
    }
    return false;
}

bool HnswElementTupleHasVectorStorage(HnswElementTuple etup)
{
    HnswElementTupleV2PayloadData *payloads = NULL;

    if (!HnswElementTupleIsVectorStorage(etup)) {
        return false;
    }

    payloads = HnswElementTupleGetPayloads(etup);
    return payloads->refMask == HNSW_ELEMENT_REF_RAW && payloads->reserved == 0 &&
        payloads->raw.kind == VEC_PAYLOAD_RAW_VECTOR &&
        ItemPointerIsValid(&payloads->raw.tid) &&
        payloads->raw.payloadLen >= VARHDRSZ &&
        HnswPayloadDiskRefReservedIsZero(&payloads->raw) &&
        !ItemPointerIsValid(&payloads->aux.tid) && payloads->aux.payloadLen == 0 &&
        payloads->aux.kind == 0 && HnswPayloadDiskRefReservedIsZero(&payloads->aux);
}

void HnswSetElementTupleV2(HnswElementTuple etup, HnswElement element, const VecPayloadDiskRef *rawRef)
{
    HnswElementTupleV2PayloadData *payloads = NULL;
    errno_t rc = EOK;

    Assert(etup != NULL);
    Assert(element != NULL);
    Assert(rawRef != NULL);

    etup->type = HNSW_ELEMENT_TUPLE_TYPE;
    etup->level = element->level;
    etup->deleted = element->deleted;
    etup->version = element->version;
    etup->unused = HNSW_ELEMENT_FLAG_VECTOR_STORAGE;
    for (int i = 0; i < HNSW_HEAPTIDS; i++) {
        if (i < element->heaptidsLength) {
            etup->heaptids[i] = element->heaptids[i];
        } else {
            ItemPointerSetInvalid(&etup->heaptids[i]);
        }
    }

    payloads = HnswElementTupleGetPayloads(etup);
    rc = memset_s(payloads, sizeof(*payloads), 0, sizeof(*payloads));
    securec_check(rc, "\0", "\0");

    payloads->refMask = HNSW_ELEMENT_REF_RAW;
    rc = memcpy_s(&payloads->raw, sizeof(payloads->raw), rawRef, sizeof(*rawRef));
    if (rc != EOK) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to copy HNSW vector storage disk reference")));
    }
}
