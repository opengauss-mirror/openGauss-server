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
 * bm25vacuum.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25vacuum.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <unordered_map>
#include "postgres.h"
#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "db4ai/bayesnet.h"
#include "access/datavec/bm25.h"
#include "storage/buf/bufmgr.h"

struct DeleteToken {
    uint32 tokenHash;
    Vector<uint32> docIds;
};

static BlockNumber FindForwrdBlock(Relation index, BlockNumber startBlkno, uint64 tokenStartIdx)
{
    Buffer cbuf;
    Page cpage;
    uint32 step = tokenStartIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
    for (uint32 i = 0; i < step; i++) {
        cbuf = ReadBuffer(index, startBlkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        startBlkno = BM25PageGetOpaque(cpage)->nextblkno;
        UnlockReleaseBuffer(cbuf);
    }
    return startBlkno;
}

static void MarkDeleteDocuments(Relation index, IndexBulkDeleteCallback callback, void *callbackState,
    Vector<BM25DocumentItem> &deleteDocs, BM25EntryPages &entryPages, BufferAccessStrategy &bas)
{
    Page page;
    Buffer buf;
    BM25DocumentMetaPageData docMetaData;
    GenericXLogState *state = nullptr;

    buf = ReadBuffer(index, entryPages.documentMetaPage);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    docMetaData = *BM25PageGetDocMeta(page);
    if (docMetaData.docCapacity == 0) {
        UnlockReleaseBuffer(buf);
        return;
    }
    UnlockReleaseBuffer(buf);

    BlockNumber curBlkno = docMetaData.startDocPage;
    while (BlockNumberIsValid(curBlkno)) {
        Buffer cbuf;
        Page cpage;
        cbuf = ReadBufferExtended(index, MAIN_FORKNUM, curBlkno, RBM_NORMAL, bas);
        LockBufferForCleanup(cbuf);

        state = GenericXLogStart(index);
        cpage = GenericXLogRegisterBuffer(state, cbuf, GENERIC_XLOG_FULL_IMAGE);
        for (int i = 0; i < BM25_DOCUMENT_MAX_COUNT_IN_PAGE; i++) {
            BM25DocumentItem *docItem =
                (BM25DocumentItem*)((char *)cpage + sizeof(PageHeaderData) + i * BM25_DOCUMENT_ITEM_SIZE);
            if (docItem->isActived && callback(&docItem->ctid.t_tid, callbackState, InvalidOid, InvalidBktId)) {
                docItem->isActived = false;
                deleteDocs.push_back(*docItem);
            }
        }
        curBlkno = BM25PageGetOpaque(cpage)->nextblkno;
        GenericXLogFinish(state);
        UnlockReleaseBuffer(cbuf);
    }
}

static void FindDocumetTokens(Relation index, Vector<BM25DocumentItem> &deleteDocs,
    unordered_map<uint32, DeleteToken> &deleteTokens, BM25EntryPages &entryPages, BufferAccessStrategy &bas)
{
    Page page;
    Buffer buf;

    buf = ReadBuffer(index, entryPages.docForwardPage);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    BM25DocForwardMetaPageData forwardData = *BM25PageGetDocForwardMeta(page);
    UnlockReleaseBuffer(buf);

    BlockNumber curBlkno = forwardData.startPage;

    for (uint32 i = 0; i < deleteDocs.size(); ++i) {
        uint64 startToken = deleteDocs[i].tokenStartIdx;
        uint64 endToken = deleteDocs[i].tokenEndIdx;
        uint32 preBlockIndex = startToken / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
        curBlkno = FindForwrdBlock(index, forwardData.startPage, startToken);
        Buffer cbuf;
        Page cpage;
        cbuf = ReadBufferExtended(index, MAIN_FORKNUM, curBlkno, RBM_NORMAL, bas);
        LockBufferForCleanup(cbuf);
        while (startToken <= endToken) {
            if (startToken / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE != preBlockIndex) {
                curBlkno = BM25PageGetOpaque(cpage)->nextblkno;
                preBlockIndex = startToken / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
                UnlockReleaseBuffer(cbuf);
                cbuf = ReadBufferExtended(index, MAIN_FORKNUM, curBlkno, RBM_NORMAL, bas);
                LockBufferForCleanup(cbuf);
            }
            cpage = BufferGetPage(cbuf);
            uint16 offset = startToken % BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
            BM25DocForwardItem *forwardItem = (BM25DocForwardItem*)(
                (char *)cpage + sizeof(PageHeaderData) + offset * BM25_DOCUMENT_FORWARD_ITEM_SIZE);
            if (deleteTokens.find(forwardItem->tokenId) == deleteTokens.end()) {
                DeleteToken deleteToken;
                deleteToken.tokenHash = forwardItem->tokenHash;
                deleteToken.docIds.push_back(forwardItem->docId);
                deleteTokens.insert({forwardItem->tokenId, deleteToken});
            } else {
                deleteTokens[forwardItem->tokenId].docIds.push_back(forwardItem->docId);
            }
            forwardItem->docId = BM25_INVALID_DOC_ID;
            startToken++;
        }
        UnlockReleaseBuffer(cbuf);
    }
}

static bool FindDocId(DeleteToken &deleteToken, uint32 docId)
{
    for (size_t idx = 0; idx < deleteToken.docIds.size(); ++idx) {
        if (deleteToken.docIds[idx] == docId) {
            return true;
        }
    }
    return false;
}

static void BM25UpdateTokenMeta(Relation index, uint32 deletedDocCount, BlockNumber tokenMetaBlkno,
    OffsetNumber tokenMetaOffno, BlockNumber insertPage)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;

    buf = ReadBuffer(index, tokenMetaBlkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    BM25TokenMetaPage tokenMeta = (BM25TokenMetaPage)PageGetItem(page, PageGetItemId(page, tokenMetaOffno));
    if (BlockNumberIsValid(insertPage) && insertPage < tokenMeta->lastInsertBlkno) {
        tokenMeta->lastInsertBlkno =  insertPage;
    }
    tokenMeta->docCount -= deletedDocCount;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
    return;
}

static void VacuumInvertedList(Relation index, uint32 tokenId, DeleteToken &deleteToken)
{
    BM25MetaPageData meta;
    BM25GetMetaPageInfo(index, &meta);
    BlockNumber hashBucketsBlkno = meta.entryPageList.hashBucketsPage;
    uint32 maxHashBucketCount = meta.entryPageList.maxHashBucketCount;

    /* bucket location */
    uint32 bucketIdx = deleteToken.tokenHash % (maxHashBucketCount * BM25_BUCKET_PAGE_ITEM_SIZE);
    Buffer cHashBucketsbuf = ReadBuffer(index, hashBucketsBlkno);
    LockBuffer(cHashBucketsbuf, BUFFER_LOCK_SHARE);
    Page cHashBucketspage = BufferGetPage(cHashBucketsbuf);
    BM25HashBucketPage bucketInfo = (BM25HashBucketPage)PageGetItem(cHashBucketspage,
        PageGetItemId(cHashBucketspage, (bucketIdx / BM25_BUCKET_PAGE_ITEM_SIZE) + 1));
    BlockNumber tokenMetasBlkno = bucketInfo->bucketBlkno[bucketIdx % BM25_BUCKET_PAGE_ITEM_SIZE];
    UnlockReleaseBuffer(cHashBucketsbuf);

    /* find tokenMeta */
    if (!BlockNumberIsValid(tokenMetasBlkno)) {
        return;
    }
    BlockNumber postingBlkno = InvalidBlockNumber;
    BlockNumber tokenMetaBlkno = InvalidBlockNumber;
    OffsetNumber tokenMetaOffno = InvalidOffsetNumber;
    Buffer cTokenMetasbuf;
    Page cTokenMetaspage;
    BlockNumber nextTokenMetasBlkno = tokenMetasBlkno;
    while (BlockNumberIsValid(nextTokenMetasBlkno)) {
        cTokenMetasbuf = ReadBuffer(index, nextTokenMetasBlkno);
        LockBuffer(cTokenMetasbuf, BUFFER_LOCK_SHARE);
        cTokenMetaspage = BufferGetPage(cTokenMetasbuf);
        OffsetNumber maxoffno = PageGetMaxOffsetNumber(cTokenMetaspage);
        for (OffsetNumber offnoTokenMeta = FirstOffsetNumber; offnoTokenMeta <= maxoffno; offnoTokenMeta++) {
            BM25TokenMetaPage tokenMeta =
                (BM25TokenMetaPage)PageGetItem(cTokenMetaspage, PageGetItemId(cTokenMetaspage, offnoTokenMeta));
            if ((tokenMeta->hashValue == deleteToken.tokenHash) && (tokenMeta->tokenId == tokenId)) {
                postingBlkno = tokenMeta->postingBlkno;
                tokenMetaBlkno = nextTokenMetasBlkno;
                tokenMetaOffno = offnoTokenMeta;
                break;
            }
        }
        nextTokenMetasBlkno = BM25PageGetOpaque(cTokenMetaspage)->nextblkno;
        UnlockReleaseBuffer(cTokenMetasbuf);
    }

    /* process posting list, delete items in docId list */
    if (!BlockNumberIsValid(postingBlkno)) {
        return;
    }

    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);
    BlockNumber nextPostingBlkno = postingBlkno;
    BlockNumber insertPage = InvalidBlockNumber;
    uint32 deletedDocCount = 0;
    while (BlockNumberIsValid(nextPostingBlkno)) {
        Buffer postingListbuf;
        Page postingListpage;
        GenericXLogState *postingState = nullptr;
        OffsetNumber deletable[MaxOffsetNumber];
        int ndeletable;

        vacuum_delay_point();

        postingListbuf = ReadBufferExtended(index, MAIN_FORKNUM, nextPostingBlkno, RBM_NORMAL, bas);
        LockBufferForCleanup(postingListbuf);
        BM25GetPage(index, &postingListpage, postingListbuf, &postingState, false);
        OffsetNumber maxoffno = PageGetMaxOffsetNumber(postingListpage);
        ndeletable = 0;
        for (OffsetNumber offnoPosting = FirstOffsetNumber; offnoPosting <= maxoffno; offnoPosting++) {
            BM25TokenPostingPage postingItem =
                (BM25TokenPostingPage)PageGetItem(postingListpage, PageGetItemId(postingListpage, offnoPosting));
            if (FindDocId(deleteToken, postingItem->docId)) {
                deletable[ndeletable++] = offnoPosting;
                deletedDocCount++;
                break;
            }
        }
        if (!BlockNumberIsValid(insertPage) && ndeletable > 0) {
            insertPage = nextPostingBlkno;
        }
        nextPostingBlkno = BM25PageGetOpaque(cTokenMetaspage)->nextblkno;
        if (ndeletable > 0) {
            /* Delete item */
            PageIndexMultiDelete(postingListpage, deletable, ndeletable);
            GenericXLogFinish(postingState);
        } else {
            GenericXLogAbort(postingState);
        }
        UnlockReleaseBuffer(postingListbuf);
    }
    if (BlockNumberIsValid(insertPage)) {
        BM25UpdateTokenMeta(index, deletedDocCount, tokenMetaBlkno, tokenMetaOffno, insertPage);
    }
    FreeAccessStrategy(bas);
    return;
}


static void DeleteTokensFromPostinglist(Relation index, unordered_map<uint32, DeleteToken> deleteTokens,
    BM25EntryPages &entryPages)
{
    for (auto iter = deleteTokens.begin(); iter != deleteTokens.end(); iter++) {
        VacuumInvertedList(index, iter->first, iter->second);
    }
}

static void UpdateBM25Statistics(Relation index, Vector<BM25DocumentItem> &deleteDocs, BM25EntryPages &entryPages)
{
    Buffer buf;
    Page page;
    BM25MetaPage metapBuf;
    GenericXLogState *state = nullptr;
    uint32 deleteDocCount = deleteDocs.size();
    uint32 deleteDocLen = 0;
    uint32 docIdx = 0;
    while (docIdx < deleteDocCount) {
        deleteDocLen += deleteDocs[docIdx].docLength;
        ++docIdx;
    }

    buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &page, buf, &state, false);
    metapBuf = BM25PageGetMeta(page);
    if (unlikely(metapBuf->magicNumber != BM25_MAGIC_NUMBER))
        elog(ERROR, "bm25 index is not valid");
    metapBuf->documentCount -= deleteDocCount;
    metapBuf->tokenCount -= deleteDocLen;
    BM25CommitBuf(buf, &state, false);
}

static void BulkDeleteDocuments(Relation index, IndexBulkDeleteCallback callback, void *callbackState)
{
    BM25MetaPageData meta;
    BM25GetMetaPageInfo(index, &meta);
    BM25EntryPages entryPages = meta.entryPageList;
    Vector<BM25DocumentItem> deleteDocs;
    unordered_map<uint32, DeleteToken> deleteTokens;
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

    MarkDeleteDocuments(index, callback, callbackState, deleteDocs, entryPages, bas);

    if (deleteDocs.size() == 0) {
        return;
    }

    FindDocumetTokens(index, deleteDocs, deleteTokens, entryPages, bas);

    DeleteTokensFromPostinglist(index, deleteTokens, entryPages);

    UpdateBM25Statistics(index, deleteDocs, entryPages);

    FreeAccessStrategy(bas);
}

/*
 * Bulk delete tuples from the index
 */
IndexBulkDeleteResult *bm25bulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
    IndexBulkDeleteCallback callback, void *callbackState)
{
    Relation rel = info->index;

    if (stats == NULL) {
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));
    }

    BulkDeleteDocuments(rel, callback, callbackState);
    return stats;
}

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *bm25vacuumcleanup_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation rel = info->index;

    if (info->analyze_only)
        return stats;

    /* stats is NULL if ambulkdelete not called */
    /* OK to return NULL if index not changed */
    if (stats == NULL) {
        return NULL;
    }

    stats->num_pages = RelationGetNumberOfBlocks(rel);

    return stats;
}