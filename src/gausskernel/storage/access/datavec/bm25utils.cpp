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
#include "tokenizer.h"

slock_t newBufferMutex;

BM25TokenizedDocData BM25DocumentTokenize(const char* doc)
{
    uint32 docLength = 0;
    EmbeddingMap embeddingMap{0};
    ConvertString2Embedding(doc, &embeddingMap, true);
    BM25TokenizedDocData tokenizedData = {};
    BM25TokenData* tokenDatas = (BM25TokenData*)palloc0(sizeof(BM25TokenData) * embeddingMap.size);
    for (size_t idx = 0; idx < embeddingMap.size; idx++) {
        tokenDatas[idx].hashValue = embeddingMap.tokens[idx].key;
        tokenDatas[idx].tokenFreq = embeddingMap.tokens[idx].value;
        errno_t rc = strncpy_s(tokenDatas[idx].tokenValue, BM25_MAX_TOKEN_LEN, embeddingMap.tokens[idx].token,
            BM25_MAX_TOKEN_LEN - 1);
        if (rc != EOK) {
            pfree(tokenDatas);
            tokenDatas = nullptr;
            docLength = 0;
            embeddingMap.size = 0;
            break;
        }
        tokenDatas[idx].tokenId = 0;
        docLength += embeddingMap.tokens[idx].value;
    }
    tokenizedData.tokenDatas = tokenDatas;
    tokenizedData.tokenCount = embeddingMap.size;
    tokenizedData.docLength = docLength;
    if (embeddingMap.tokens != nullptr) {
        free(embeddingMap.tokens);
        embeddingMap.tokens = nullptr;
    }
    return tokenizedData;
}

/*
 * New buffer
 */
Buffer BM25NewBuffer(Relation index, ForkNumber forkNum)
{
    SpinLockAcquire(&newBufferMutex);
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    SpinLockRelease(&newBufferMutex);
    return buf;
}

/*
 * Init page
 */
void BM25InitPage(Buffer buf, Page page)
{
    PageInit(page, BufferGetPageSize(buf), sizeof(BM25PageOpaqueData));
    BM25PageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    BM25PageGetOpaque(page)->page_id = BM25_PAGE_ID;
}

/*
 * Init and register page
 */
void BM25InitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
    *state = GenericXLogStart(index);
    *page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
    BM25InitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void BM25CommitBuffer(Buffer buf, GenericXLogState *state)
{
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void BM25AppendPage(Relation index, Buffer *buf, Page *page, ForkNumber forkNum, bool unlockOldBuf)
{
    /* Get new buffer */
    Buffer newbuf = BM25NewBuffer(index, forkNum);
    Page newpage = BufferGetPage(newbuf);

    /* Update the previous buffer */
    BM25PageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

    /* Init new page */
    BM25InitPage(newbuf, newpage);

    MarkBufferDirty(*buf);
    if (unlockOldBuf) {
        /* Unlock */
        UnlockReleaseBuffer(*buf);
    }

    *page = BufferGetPage(newbuf);
    *buf = newbuf;
}

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

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    docId = metapBuf->nextDocId;
    metapBuf->nextDocId++;
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
    return docId;
}

uint32 BM25AllocateTokenId(Relation index)
{
    Buffer buf;
    Page page;
    BM25MetaPage metapBuf;
    uint32 tokenId;

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    tokenId = metapBuf->nextTokenId;
    metapBuf->nextTokenId++;
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
    return tokenId;
}

void BM25IncreaseDocAndTokenCount(Relation index, uint32 tokenCount, float &avgdl)
{
    Buffer buf;
    Page page;
    BM25MetaPage metapBuf;

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    metapBuf->documentCount++;
    metapBuf->tokenCount += tokenCount;
    avgdl = metapBuf->tokenCount / metapBuf->documentCount;
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

BlockNumber SeekBlocknoForDoc(Relation index, uint32 docId, BlockNumber startBlkno, BlockNumber step)
{
    Buffer buf;
    Page page;
    BlockNumber docBlkno = startBlkno;
    for (int i = 0; i < step; ++i) {
        if (unlikely(BlockNumberIsValid(docBlkno))) {
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

bool FindHashBucket(uint32 bucketId, BM25PageLocationInfo &bucketLocation, Buffer buf, Page page)
{
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
        BM25HashBucketPage bucket = (BM25HashBucketPage)PageGetItem(page, PageGetItemId(page, offno));
        if (bucket->bucketId == bucketId) {
            bucketLocation.blkno = BufferGetBlockNumber(buf);
            bucketLocation.offno = offno;
            return true;
        }
    }
    return false;
}

bool FindTokenMeta(BM25TokenData &tokenData, BM25PageLocationInfo &tokenMetaLocation, Buffer buf, Page page)
{
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
        BM25TokenMetaPage tokenMeta = (BM25TokenMetaPage)PageGetItem(page, PageGetItemId(page, offno));
        if (strncmp(tokenMeta->token, tokenData.tokenValue, BM25_MAX_TOKEN_LEN - 1) == 0) {
            tokenMetaLocation.blkno = BufferGetBlockNumber(buf);
            tokenMetaLocation.offno = offno;
            return true;
        }
    }
    return false;
}
