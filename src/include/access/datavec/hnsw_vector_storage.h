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
 * hnsw_vector_storage.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/hnsw_vector_storage.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HNSW_VECTOR_STORAGE_H
#define HNSW_VECTOR_STORAGE_H

#include "postgres.h"

#include "access/datavec/hnsw.h"

typedef enum HnswVectorStorageMetaLayout {
    HNSW_VECTOR_STORAGE_META_INVALID = 0,
    HNSW_VECTOR_STORAGE_META_LEGACY,
    HNSW_VECTOR_STORAGE_META_V2
} HnswVectorStorageMetaLayout;

HnswVectorStorageMetaLayout HnswClassifyVectorStorageMeta(HnswMetaPage metap, uint32 *payloadLen);
bool HnswGetVectorStorageGraphHeadBlkno(HnswMetaPage metap, BlockNumber *graphHeadBlkno);
bool HnswRelationHasVectorPayloadStorage(
    Relation index, BlockNumber *payloadInsertBlkno, uint32 *payloadLen);
/* True when the element tuple carries the V2 storage flag. Payload tid may still be invalid. */
static inline bool HnswElementTupleIsVectorStorage(HnswElementTuple etup)
{
    return etup != NULL && HnswIsElementTuple(etup) &&
        (etup->unused & HNSW_ELEMENT_FLAG_VECTOR_STORAGE) != 0;
}

static inline HnswElementTupleV2PayloadData *HnswElementTupleGetPayloads(HnswElementTuple etup)
{
    return (HnswElementTupleV2PayloadData *)((char *)etup + offsetof(HnswElementTupleData, data));
}

bool HnswElementTupleMatchesMetaLayout(HnswElementTuple etup, HnswVectorStorageMetaLayout layout);
/* True when the tuple is V2 and the embedded raw payload tid/kind/length are well-formed. */
bool HnswElementTupleHasVectorStorage(HnswElementTuple etup);
void HnswSetElementTupleV2(HnswElementTuple etup, HnswElement element, const VecPayloadDiskRef *rawRef);

#endif /* HNSW_VECTOR_STORAGE_H */
