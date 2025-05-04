/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * bm25utils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25utils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/datavec/bitvec.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "access/datavec/halfutils.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/bm25.h"
#include "access/datavec/utils.h"
#include "storage/buf/bufmgr.h"

/*
 * Get the metapage info
 */
void BM25GetMetaPageInfo(Relation index, BM25MetaPage metap)
{
    Buffer buf;
    Page page;
    BM25MetaPage metapBuf;

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    errno_t rc = memcpy_s(metap, sizeof(BM25MetaPageData), metapBuf, sizeof(BM25MetaPageData));
    securec_check(rc, "\0", "\0");
    UnlockReleaseBuffer(buf);
}

uint32 BM25AllocateDocId(Relation index)
{
    Buffer buf;
    Page page;
    BM25MetaPage metapBuf;
    uint32 docId;
    GenericXLogState *state;

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    docId = metapBuf->nextDocId;
    metapBuf->nextDocId++;
    BM25CommitBuffer(buf, state);
    return docId;
}

uint32 BM25AllocateTokenId(Relation index)
{
    Buffer buf;
    Page page;
    BM25MetaPage metapBuf;
    uint32 tokenId;
    GenericXLogState *state;

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    tokenId = metapBuf->nextTokenId;
    metapBuf->nextTokenId++;
    BM25CommitBuffer(buf, state);
    return tokenId;
}

void BM25IncreaseDocAndTokenCount(Relation index, uint32 tokenCount, float &avgdl)
{
    Buffer buf;
    Page page;
    BM25MetaPage metapBuf;
    GenericXLogState *state;

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    metapBuf->documentCount++;
    metapBuf->tokenCount += tokenCount;
    avgdl = metapBuf->tokenCount / metapBuf->documentCount;
    BM25CommitBuffer(buf, state);
}

BlockNumber SeekBlocknoForDoc(Relation index, uint32 docId, BlockNumber startBlkno, BlockNumber step)
{
    Buffer buf;
    Page page;
    BlockNumber docBlkno = startBlkno;
    for (int i = 0; i < step; ++i) {
        if (unlikely(BlockNumberIsValid(docBlkno)) {
            elog(ERROR, "SeekBlocknoForDoc: Invalid Block Number.");
        }
        buf = ReadBuffer(index, docBlkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        docBlkno = BM25PageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
    }
    return docBlkno;
}