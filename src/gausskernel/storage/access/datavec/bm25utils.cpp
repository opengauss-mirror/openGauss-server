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

BM25TokenizedDocData BM25DocumentTokenize(const char* doc, bool cutForSearch)
{
    uint32 docLength = 0;
    EmbeddingMap embeddingMap{0};
    ConvertString2Embedding(doc, &embeddingMap, true, cutForSearch);
    BM25TokenizedDocData tokenizedData = {};
    BM25TokenData* tokenDatas = (BM25TokenData*)palloc0(sizeof(BM25TokenData) * embeddingMap.size);
    for (size_t idx = 0; idx < embeddingMap.size; idx++) {
        tokenDatas[idx].hashValue = embeddingMap.tokens[idx].key;
        tokenDatas[idx].tokenFreq = embeddingMap.tokens[idx].value;
        errno_t rc = strncpy_s(tokenDatas[idx].tokenValue, BM25_MAX_TOKEN_LEN, embeddingMap.tokens[idx].token,
            BM25_MAX_TOKEN_LEN - 1);
        if (rc != EOK) {
            pfree(tokenDatas);
            if (embeddingMap.tokens != nullptr) {
                free(embeddingMap.tokens);
                embeddingMap.tokens = nullptr;
            }
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
    Buffer lockBuf = ReadBuffer(index, BM25_LOCK_BLKNO);
    LockBuffer(lockBuf, BUFFER_LOCK_EXCLUSIVE);

    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    UnlockReleaseBuffer(lockBuf);
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

void BM25GetPage(Relation index, Page *page, Buffer buf, GenericXLogState **state, bool building)
{
    if (building) {
        *state = nullptr;
        *page = BufferGetPage(buf);
    } else {
        *state = GenericXLogStart(index);
        *page = GenericXLogRegisterBuffer(*state, buf, GENERIC_XLOG_FULL_IMAGE);
    }
    return;
}

void BM25CommitBuf(Buffer buf, GenericXLogState **state, bool building, bool releaseBuf)
{
    if (building) {
        MarkBufferDirty(buf);
    } else {
        GenericXLogFinish(*state);
    }
    if (releaseBuf) {
        UnlockReleaseBuffer(buf);
    }
    return;
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void BM25AppendPage(Relation index, Buffer *buf, Page *page, ForkNumber forkNum, GenericXLogState **state,
    bool building)
{
    /* Get new buffer */
    Buffer newbuf = BM25NewBuffer(index, forkNum);
    Page newpage;

    /* Update the previous buffer */
    BM25PageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);
    BM25CommitBuf(*buf, state, building);

    /* Init new page */
    BM25GetPage(index, &newpage, newbuf, state, building);
    BM25InitPage(newbuf, newpage);
    BM25CommitBuf(newbuf, state, building, false);

    BM25GetPage(index, page, newbuf, state, building);
    *buf = newbuf;
    return;
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

uint32 BM25AllocateNewDocId(Relation index, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    BM25MetaPage metapBuf;
    uint32 docId;

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &page, buf, &state, building);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    docId = metapBuf->nextDocId;
    if (unlikely(docId == BM25_INVALID_DOC_ID)) {
        elog(ERROR, "bm25 doc id exhausted, please rebuild index.");
    }

    metapBuf->nextDocId++;
    BM25CommitBuf(buf, &state, building);
    return docId;
}

uint32 TryAllocateDocIdFromFreePage(Relation index, uint32 docTokenCount)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    BM25MetaPageData meta;
    BM25GetMetaPageInfo(index, &meta);
    BM25EntryPages entryPages = meta.entryPageList;
    uint32 docId = BM25_INVALID_DOC_ID;
    bool allocated = false;

    BlockNumber curFreePage = entryPages.docmentFreePage;
    while (BlockNumberIsValid(curFreePage)) {
        buf = ReadBuffer(index, curFreePage);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &page, buf, &state, false);
        OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            BM25FreeDocumentItem* freeItem = (BM25FreeDocumentItem*)PageGetItem(page, PageGetItemId(page, offno));
            if (freeItem->tokenCapacity >= docTokenCount) {
                docId = freeItem->docId;
                freeItem->docId = BM25_INVALID_DOC_ID;
                freeItem->tokenCapacity = 0;
                allocated = true;
                PageIndexTupleDelete(page, offno);
                break;
            }
        }
        if (allocated) {
            BM25CommitBuf(buf, &state, false);
            break;
        }
        curFreePage = BM25PageGetOpaque(page)->nextblkno;
        BM25CommitBuf(buf, &state, false);
    }
    return docId;
}

uint32 BM25AllocateDocId(Relation index, bool building, uint32 docTokenCount)
{
    uint32 docId = BM25_INVALID_DOC_ID;
    if (building) {
        return BM25AllocateNewDocId(index, building);
    }
    docId = TryAllocateDocIdFromFreePage(index, docTokenCount);
    if (docId == BM25_INVALID_DOC_ID) {
        return BM25AllocateNewDocId(index, building);
    }
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

void BM25IncreaseDocAndTokenCount(Relation index, uint32 tokenCount, float &avgdl, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    BM25MetaPage metapBuf;

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &page, buf, &state, building);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    metapBuf->documentCount++;
    metapBuf->tokenCount += tokenCount;
    avgdl = metapBuf->tokenCount / metapBuf->documentCount;
    BM25CommitBuf(buf, &state, building);
}

void RecordDocBlkno2DocBlknoTable(Relation index, BM25DocMetaPage docMetaPage,
    BlockNumber newDocBlkno, bool building, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    BlockNumber curTableBlkno = docMetaPage->docBlknoInsertPage;
    OffsetNumber offno;
    GenericXLogState *state = nullptr;
    uint32 itemSize = MAXALIGN(sizeof(BlockNumber));

    /* first page */
    if (!BlockNumberIsValid(curTableBlkno)) {
        buf = BM25NewBuffer(index, forkNum);
        BM25GetPage(index, &page, buf, &state, building);
        BM25InitPage(buf, page);
        curTableBlkno = BufferGetBlockNumber(buf);
        docMetaPage->docBlknoTable = curTableBlkno;
        docMetaPage->docBlknoInsertPage = curTableBlkno;
    } else {
        buf = ReadBuffer(index, curTableBlkno);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &page, buf, &state, building);
        if (PageGetFreeSpace(page) < itemSize) {
            BM25AppendPage(index, &buf, &page, forkNum, &state, building);
            curTableBlkno = BufferGetBlockNumber(buf);
            docMetaPage->docBlknoInsertPage = curTableBlkno;
        }
    }
    offno = PageAddItem(page, (Item)(&newDocBlkno), itemSize, InvalidOffsetNumber, false, false);
    if (offno == InvalidOffsetNumber) {
        if (!building)
            GenericXLogAbort(state);
        UnlockReleaseBuffer(buf);
        elog(ERROR, "failed to add doc blkno item [DocBlknoTable] to \"%s\"", RelationGetRelationName(index));
    }
    BM25CommitBuf(buf, &state, building);
}

BlockNumber SeekBlocknoForDoc(Relation index, uint32 docId, BlockNumber docBlknoTable)
{
    Buffer buf;
    Page page;
    BlockNumber curTableBlkno = docBlknoTable;
    BlockNumber docBlkno = InvalidBlockNumber;
    uint32 scanedDocBlknoNum = 0;
    uint32 docBlknoIndex = docId / BM25_DOCUMENT_MAX_COUNT_IN_PAGE;

    /* insert or search */
    /* get doc block number in docBlknoTable */
    while (BlockNumberIsValid(curTableBlkno)) {
        OffsetNumber maxoffno;
        buf = ReadBuffer(index, curTableBlkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        maxoffno = PageGetMaxOffsetNumber(page);
        if (scanedDocBlknoNum + maxoffno >= docBlknoIndex + 1) {
            uint16 offset = docBlknoIndex + 1 - scanedDocBlknoNum;
            docBlkno = *((BlockNumber*)PageGetItem(page, PageGetItemId(page, offset)));
            UnlockReleaseBuffer(buf);
            break;
        }
        scanedDocBlknoNum += maxoffno;
        curTableBlkno = BM25PageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
    }

    if (unlikely(!BlockNumberIsValid(docBlkno))) {
        elog(ERROR, "Failed to search doc blkno for \"%s\"", RelationGetRelationName(index));
    }

    return docBlkno;
}

bool FindTokenMeta(BM25TokenData &tokenData, BM25PageLocationInfo &tokenMetaLocation, Buffer buf, Page page)
{
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
        BM25TokenMetaPage tokenMeta = (BM25TokenMetaPage)PageGetItem(page, PageGetItemId(page, offno));
        if ((tokenMeta->hashValue == tokenData.hashValue) &&
			(strncmp(tokenMeta->token, tokenData.tokenValue, BM25_MAX_TOKEN_LEN - 1) == 0)) {
            tokenMetaLocation.blkno = BufferGetBlockNumber(buf);
            tokenMetaLocation.offno = offno;
            return true;
        }
    }
    return false;
}

void RecordDocForwardBlkno2DocForwardBlknoTable(Relation index, BM25DocForwardMetaPage metaForwardPage,
    BlockNumber newDocForwardBlkno, bool building, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    BlockNumber curTableBlkno = metaForwardPage->docForwardBlknoInsertPage;
    OffsetNumber offno;
    GenericXLogState *state = nullptr;
    uint32 itemSize = MAXALIGN(sizeof(BlockNumber));

    /* first page */
    if (!BlockNumberIsValid(curTableBlkno)) {
        buf = BM25NewBuffer(index, forkNum);
        BM25GetPage(index, &page, buf, &state, building);
        BM25InitPage(buf, page);
        curTableBlkno = BufferGetBlockNumber(buf);
        metaForwardPage->docForwardBlknoTable = curTableBlkno;
        metaForwardPage->docForwardBlknoInsertPage = curTableBlkno;
    } else {
        buf = ReadBuffer(index, curTableBlkno);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &page, buf, &state, building);
        if (PageGetFreeSpace(page) < itemSize) {
            BM25AppendPage(index, &buf, &page, forkNum, &state, building);
            curTableBlkno = BufferGetBlockNumber(buf);
            metaForwardPage->docForwardBlknoInsertPage = curTableBlkno;
        }
    }
    offno = PageAddItem(page, (Item)(&newDocForwardBlkno), itemSize, InvalidOffsetNumber, false, false);
    if (offno == InvalidOffsetNumber) {
        if (!building)
            GenericXLogAbort(state);
        UnlockReleaseBuffer(buf);
        elog(ERROR, "failed to add doc forward blkno item [DocForwardBlknoTable] to \"%s\"",
            RelationGetRelationName(index));
    }
    BM25CommitBuf(buf, &state, building);
}

BlockNumber SeekBlocknoForForwardToken(Relation index, uint32 forwardIdx, BlockNumber docForwardBlknoTable)
{
    Buffer buf;
    Page page;
    BlockNumber curTableBlkno = docForwardBlknoTable;
    BlockNumber docForwardBlkno = InvalidBlockNumber;
    uint32 scanedForwardBlknoNum = 0;
    uint32 docForwardBlknoIndex = forwardIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE + 1;

    while (BlockNumberIsValid(curTableBlkno)) {
        OffsetNumber maxoffno;
        buf = ReadBuffer(index, curTableBlkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        maxoffno = PageGetMaxOffsetNumber(page);
        if (scanedForwardBlknoNum + maxoffno >= docForwardBlknoIndex) {
            uint16 offset = docForwardBlknoIndex - scanedForwardBlknoNum;
            docForwardBlkno = *((BlockNumber*)PageGetItem(page, PageGetItemId(page, offset)));
            UnlockReleaseBuffer(buf);
            break;
        }
        scanedForwardBlknoNum += maxoffno;
        curTableBlkno = BM25PageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
    }

    if (unlikely(!BlockNumberIsValid(docForwardBlkno))) {
        elog(ERROR, "Failed to search doc forward blkno for \"%s\"", RelationGetRelationName(index));
    }

    return docForwardBlkno;
}

void BM25BatchInsertRecord(Relation index)
{
    Oid indexOid = RelationGetRelid(index);
    knl_u_bm25_context* bm25_ctx = &u_sess->bm25_ctx;

    /* means insert the second tuple */
    if (unlikely(indexOid == bm25_ctx->indexOidForCount && bm25_ctx->insertTupleNum == 1)) {
        bm25_ctx->isFirstTuple = false;
    }

    /* record first index oid and count tuple num */
    if (unlikely(!OidIsValid(bm25_ctx->indexOidForCount))) {
        bm25_ctx->insertXid = GetCurrentTransactionIdIfAny();
        bm25_ctx->indexOidForCount = indexOid;
        bm25_ctx->insertTupleNum += 1;
        bm25_ctx->indexOids = lappend_oid(bm25_ctx->indexOids, indexOid);
        return;
    }

    if (likely(bm25_ctx->indexOidForCount == indexOid)) {
        bm25_ctx->insertTupleNum += 1;
    } else if (bm25_ctx->isFirstTuple) {
        bm25_ctx->indexOids = lappend_oid(bm25_ctx->indexOids, indexOid);
    }
}

void BM25BatchInsertResetRecord()
{
    list_free_ext(u_sess->bm25_ctx.indexOids);
    u_sess->bm25_ctx.indexOidForCount = InvalidOid;
    u_sess->bm25_ctx.insertTupleNum = 0;
    u_sess->bm25_ctx.insertXid = InvalidTransactionId;
    u_sess->bm25_ctx.isFirstTuple = true;
}

void BM25BatchInsertAbort()
{
    Oid indexOid;
    Relation index;
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    BM25MetaPage metapBuf;
    ListCell *lc = NULL;

    if (u_sess->bm25_ctx.indexOids == NIL) {
        return;
    }

    foreach (lc, u_sess->bm25_ctx.indexOids) {
        Oid indexOid = (Oid)lfirst_oid(lc);
        index = index_open(indexOid, NoLock);
        buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &page, buf, &state, false);
        metapBuf = BM25PageGetMeta(page);
        if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
            elog(ERROR, "bm25 index is not valid");
        metapBuf->lastBacthInsertFailed = true;
        BM25CommitBuf(buf, &state, false);
        index_close(index, NoLock);
    }
    BM25BatchInsertResetRecord();
}
