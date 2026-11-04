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
#include "access/datavec/varblock.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/bufpage.h"
#include "storage/indexfsm.h"

struct DeleteToken {
    uint32 tokenHash;
    Vector<uint32> docIds;
};

/* Token meta page slot + outputs for vacuum locate (G.FUD.05) */
typedef struct BM25TokenMetaPageRef {
    BM25TokenMetaPage tokenMeta;
    BlockNumber pageBlkno;
    OffsetNumber offno;
} BM25TokenMetaPageRef;

typedef struct BM25TokenMetaVacuumLocate {
    BlockNumber postingBlkno;
    ItemPointerData varBlockChainHead;
    uint32 tokenMetaDocCount;
    BlockNumber tokenMetaBlkno;
    OffsetNumber tokenMetaOffno;
} BM25TokenMetaVacuumLocate;

typedef struct VarBlockVacuumCollector {
    VarBlockReadContext ctx;
    Vector<BM25TokenPostingItem> *keptItems;
    DeleteToken *deleteToken;
    uint32 totalItems;
} VarBlockVacuumCollector;

static bool FindDocId(DeleteToken &deleteToken, uint32 docId);

static void VarBlockVacuumCollectCallback(const VarBlockChunkHeader *hdr, const char *payload, VarBlockReadContext *arg)
{
    VarBlockVacuumCollector *coll = (VarBlockVacuumCollector *)arg;
    uint32 len = hdr->payload_len;
    for (uint32 i = 0; i + BM25_POSTING_ITEM_ALIGNED_SIZE <= len; i += BM25_POSTING_ITEM_ALIGNED_SIZE) {
        const BM25TokenPostingItem *item = (const BM25TokenPostingItem *)(payload + i);
        coll->totalItems++;
        if (!FindDocId(*coll->deleteToken, item->docId)) {
            coll->keptItems->push_back(*item);
        }
    }
}

static int VarBlockVacuumComparePosting(const void *a, const void *b)
{
    uint32 da = ((const BM25TokenPostingItem *)a)->docId;
    uint32 db = ((const BM25TokenPostingItem *)b)->docId;
    return (da < db) ? -1 : (da > db) ? 1 : 0;
}

static void VarBlockAppendPostingTail(Relation index, ForkNumber forkNum, ItemPointerData *tailCtid,
    const BM25TokenPostingItem *item)
{
    BlockNumber blkno = ItemPointerGetBlockNumber(tailCtid);
    OffsetNumber offno = ItemPointerGetOffsetNumber(tailCtid);
    Buffer buf = ReadBufferExtended(index, forkNum, blkno, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buf);
    ItemId id = PageGetItemId(page, offno);
    VarBlockChunkHeader *hdr = (VarBlockChunkHeader *)PageGetItem(page, id);
    Size chunkTotal = VarBlockSize(hdr->level);
    Size payloadCapacity = chunkTotal - sizeof(VarBlockChunkHeader);

    if (hdr->payload_len + BM25_POSTING_ITEM_ALIGNED_SIZE > payloadCapacity) {
        UnlockReleaseBuffer(buf);
        ItemPointerData newTail = VarBlockExtendChain(index, forkNum, tailCtid, false);
        *tailCtid = newTail;
        VarBlockAppendPostingTail(index, forkNum, tailCtid, item);
        return;
    }

    GenericXLogState *state = GenericXLogStart(index);
    Page wpage = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    ItemId wid = PageGetItemId(wpage, offno);
    VarBlockChunkHeader *whdr = (VarBlockChunkHeader *)PageGetItem(wpage, wid);
    char *payload = (char *)whdr + sizeof(VarBlockChunkHeader);
    errno_t rc = memcpy_s(payload + whdr->payload_len, (size_t)(payloadCapacity - whdr->payload_len),
        item, BM25_POSTING_ITEM_ALIGNED_SIZE);
    if (rc != EOK) {
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errmsg("VarBlock vacuum append: memcpy_s failed")));
    }
    whdr->payload_len += (uint16)BM25_POSTING_ITEM_ALIGNED_SIZE;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

static bool BuildVarBlockChainFromKeptItems(Relation index, Vector<BM25TokenPostingItem> &keptItems,
    ItemPointerData *newHead, ItemPointerData *newTail)
{
    if (keptItems.size() == 0) {
        return false;
    }

    qsort(keptItems.begin(), keptItems.size(), sizeof(BM25TokenPostingItem), VarBlockVacuumComparePosting);
    *newHead = VarBlockAllocFirstChunk(index, MAIN_FORKNUM, 0, false);
    *newTail = *newHead;
    BM25TokenPostingItem *items = keptItems.begin();
    for (size_t i = 0; i < keptItems.size(); i++) {
        VarBlockAppendPostingTail(index, MAIN_FORKNUM, newTail, &items[i]);
    }
    return true;
}

static void MarkDeleteDocuments(Relation index, IndexBulkDeleteCallback callback, void *callbackState,
    Vector<BM25DocumentItem> &deleteDocs, BM25EntryPages &entryPages, BufferAccessStrategy &bas, uint32 indexVersion)
{
    Page page;
    Buffer buf;
    Buffer cbuf;
    Page cpage;
    BM25DocumentMetaPageData docMetaData;
    GenericXLogState *state = nullptr;
    const Size docAreaOff = BM25PageDocumentAreaOffset(indexVersion);

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
        cbuf = ReadBufferExtended(index, MAIN_FORKNUM, curBlkno, RBM_NORMAL, bas);
        LockBufferForCleanup(cbuf);

        state = GenericXLogStart(index);
        cpage = GenericXLogRegisterBuffer(state, cbuf, GENERIC_XLOG_FULL_IMAGE);
        for (uint32 i = 0; i < BM25_DOCUMENT_MAX_COUNT_IN_PAGE; i++) {
            BM25DocumentItem *docItem =
                (BM25DocumentItem*)((char *)cpage + docAreaOff + i * BM25_DOCUMENT_ITEM_SIZE);
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
    unordered_map<uint32, DeleteToken> &deleteTokens, BM25EntryPages &entryPages, BufferAccessStrategy &bas,
    uint32 indexVersion)
{
    Page page;
    Buffer buf;
    Buffer cbuf;
    Page cpage;
    GenericXLogState *state = nullptr;
    BlockNumber curBlkno;
    BlockNumber preBlockIdx;
    BlockNumber curBlockIdx;
    uint64 startTokenIdx;
    uint64 endTokenIdx;
    uint16 offset;
    BM25DocForwardItem *forwardItem = nullptr;
    const Size docAreaOff = BM25PageDocumentAreaOffset(indexVersion);

    /* read forward meta data */
    buf = ReadBuffer(index, entryPages.docForwardPage);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    BM25DocForwardMetaPageData forwardData = *BM25PageGetDocForwardMeta(page);
    UnlockReleaseBuffer(buf);

    /* find all doc tokens */
    for (uint32 i = 0; i < deleteDocs.size(); ++i) {
        startTokenIdx = deleteDocs[i].tokenStartIdx;
        endTokenIdx = deleteDocs[i].tokenEndIdx;
        curBlkno = SeekBlocknoForForwardToken(index, startTokenIdx, forwardData.docForwardBlknoTable);
        preBlockIdx = startTokenIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;

        cbuf = ReadBufferExtended(index, MAIN_FORKNUM, curBlkno, RBM_NORMAL, bas);
        LockBufferForCleanup(cbuf);
        state = GenericXLogStart(index);
        cpage = GenericXLogRegisterBuffer(state, cbuf, GENERIC_XLOG_FULL_IMAGE);
        while (startTokenIdx <= endTokenIdx) {
            curBlockIdx = startTokenIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
            if (curBlockIdx != preBlockIdx) {
                curBlkno = BM25PageGetOpaque(cpage)->nextblkno;
                preBlockIdx = startTokenIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;

                GenericXLogFinish(state);
                UnlockReleaseBuffer(cbuf);

                cbuf = ReadBufferExtended(index, MAIN_FORKNUM, curBlkno, RBM_NORMAL, bas);
                LockBufferForCleanup(cbuf);
                state = GenericXLogStart(index);
                cpage = GenericXLogRegisterBuffer(state, cbuf, GENERIC_XLOG_FULL_IMAGE);
            }
            offset = startTokenIdx % BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
            forwardItem =
                (BM25DocForwardItem*)((char *)cpage + docAreaOff + offset * BM25_DOCUMENT_FORWARD_ITEM_SIZE);
            if (forwardItem->docId == BM25_INVALID_DOC_ID) {
                break;
            }
            if (deleteTokens.find(forwardItem->tokenId) == deleteTokens.end()) {
                DeleteToken deleteToken;
                deleteToken.tokenHash = forwardItem->tokenHash;
                deleteToken.docIds.push_back(forwardItem->docId);
                deleteTokens.insert({forwardItem->tokenId, deleteToken});
            } else {
                deleteTokens[forwardItem->tokenId].docIds.push_back(forwardItem->docId);
            }
            forwardItem->docId = BM25_INVALID_DOC_ID;
            startTokenIdx++;
        }
        GenericXLogFinish(state);
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

static void AssignTokenMetaLocateForVacuum(const BM25MetaPageData &meta, const BM25TokenMetaPageRef *pageRef,
    BM25TokenMetaVacuumLocate *out)
{
    ItemPointerSetInvalid(&out->varBlockChainHead);
    out->postingBlkno = pageRef->tokenMeta->postingBlkno;
    if (meta.version >= BM25_VERSION_VARBLOCK_POSTING) {
        out->varBlockChainHead = pageRef->tokenMeta->postingChainHead;
    }
    out->tokenMetaDocCount = pageRef->tokenMeta->docCount;
    out->tokenMetaBlkno = pageRef->pageBlkno;
    out->tokenMetaOffno = pageRef->offno;
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
    ItemPointerData varBlockChainHead;
    ItemPointerSetInvalid(&varBlockChainHead);
    uint32 tokenMetaDocCount = 0;
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
                BM25TokenMetaPageRef pageRef = { tokenMeta, nextTokenMetasBlkno, offnoTokenMeta };
                BM25TokenMetaVacuumLocate locate;
                AssignTokenMetaLocateForVacuum(meta, &pageRef, &locate);
                postingBlkno = locate.postingBlkno;
                varBlockChainHead = locate.varBlockChainHead;
                tokenMetaDocCount = locate.tokenMetaDocCount;
                tokenMetaBlkno = locate.tokenMetaBlkno;
                tokenMetaOffno = locate.tokenMetaOffno;
                break;
            }
        }
        nextTokenMetasBlkno = BM25PageGetOpaque(cTokenMetaspage)->nextblkno;
        UnlockReleaseBuffer(cTokenMetasbuf);
    }

    /* VarBlock posting chain: collect kept items, rebuild chain, update tokenMeta */
    if (meta.version >= BM25_VERSION_VARBLOCK_POSTING && ItemPointerIsValid(&varBlockChainHead)) {
        Vector<BM25TokenPostingItem> keptItems;
        VarBlockVacuumCollector collector;
        collector.ctx.reserved = 0;
        collector.keptItems = &keptItems;
        collector.deleteToken = &deleteToken;
        collector.totalItems = 0;
        VarBlockReadChain(index, MAIN_FORKNUM, &varBlockChainHead, VarBlockVacuumCollectCallback, &collector.ctx);
        uint32 chainTotalCount = collector.totalItems;
        uint32 keptCount = (uint32)keptItems.size();
        uint32 deletedCount = (chainTotalCount >= keptCount) ? (chainTotalCount - keptCount) : 0;
        if (unlikely(tokenMetaDocCount != chainTotalCount)) {
            ereport(WARNING,
                (errmsg("bm25 varblock docCount mismatch in vacuum, tokenMeta=%u chain=%u",
                    tokenMetaDocCount, chainTotalCount)));
        }
        if (deletedCount > 0) {
            /*
             * Order matters for crash safety: build the new chain and switch tokenMeta to it
             * before freeing the old chain. If we freed first, a crash before meta commit would
             * leave tokenMeta pointing at deleted chunks; freeing after meta points to the new
             * chain only leaks old pages until a later cleanup if we crash between meta and free.
             */
            ItemPointerData oldChainHead = varBlockChainHead;
            ItemPointerData newHead;
            ItemPointerData newTail;
            bool haveNewChain = BuildVarBlockChainFromKeptItems(index, keptItems, &newHead, &newTail);
            Buffer metaBuf = ReadBuffer(index, tokenMetaBlkno);
            LockBuffer(metaBuf, BUFFER_LOCK_EXCLUSIVE);
            GenericXLogState *state = GenericXLogStart(index);
            Page metaPage = GenericXLogRegisterBuffer(state, metaBuf, GENERIC_XLOG_FULL_IMAGE);
            BM25TokenMetaPage tokenMeta =
                (BM25TokenMetaPage)PageGetItem(metaPage, PageGetItemId(metaPage, tokenMetaOffno));
            if (haveNewChain) {
                tokenMeta->postingChainHead = newHead;
                tokenMeta->postingChainTail = newTail;
            } else {
                ItemPointerSetInvalid(&tokenMeta->postingChainHead);
                ItemPointerSetInvalid(&tokenMeta->postingChainTail);
            }
            tokenMeta->docCount = keptCount;
            GenericXLogFinish(state);
            UnlockReleaseBuffer(metaBuf);

            VarBlockFreeChain(index, MAIN_FORKNUM, &oldChainHead, false);
        }
        return;
    }

    /* process posting list (page format), delete items in docId list */
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
        nextPostingBlkno = BM25PageGetOpaque(postingListpage)->nextblkno;
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

static void AddDocIdsIntoFreeList(Relation index, Vector<BM25DocumentItem> &deleteDocs, BM25EntryPages &entryPages)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    BlockNumber curFreePage = entryPages.docmentFreePage;
    uint32 docIdx = 0;
    uint32 itemSize = MAXALIGN(sizeof(BM25FreeDocumentItem));

    buf = ReadBuffer(index, curFreePage);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &page, buf, &state, false);
    while (docIdx < deleteDocs.size()) {
        if (PageGetFreeSpace(page) < itemSize) {
            curFreePage = BM25PageGetOpaque(page)->nextblkno;
            if (!BlockNumberIsValid(curFreePage)) {
                BM25AppendPage(index, &buf, &page, MAIN_FORKNUM, &state, false);
            } else {
                BM25CommitBuf(buf, &state, false);
                buf = ReadBuffer(index, curFreePage);
                LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
                BM25GetPage(index, &page, buf, &state, false);
            }
        }
        BM25FreeDocumentItem freeItem;
        freeItem.docId = deleteDocs[docIdx].docId;
        freeItem.tokenCapacity = deleteDocs[docIdx].tokenEndIdx - deleteDocs[docIdx].tokenStartIdx + 1;
        OffsetNumber offno = PageAddItem(page, (Item)(&freeItem), MAXALIGN(sizeof(BM25FreeDocumentItem)),
            InvalidOffsetNumber, false, false);
        if (offno == InvalidOffsetNumber) {
            GenericXLogAbort(state);
            UnlockReleaseBuffer(buf);
            elog(ERROR, "failed to add free doc item [BM25FreeDocumentItem] to \"%s\"", RelationGetRelationName(index));
        }
        docIdx++;
    }
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

    MarkDeleteDocuments(index, callback, callbackState, deleteDocs, entryPages, bas, meta.version);

    if (deleteDocs.size() == 0) {
        return;
    }

    FindDocumetTokens(index, deleteDocs, deleteTokens, entryPages, bas, meta.version);

    DeleteTokensFromPostinglist(index, deleteTokens, entryPages);

    UpdateBM25Statistics(index, deleteDocs, entryPages);

    AddDocIdsIntoFreeList(index, deleteDocs, entryPages);

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

    /*
     * Clean up trailing placeholder chunks left by VarBlockFreeChain.
     * This must run before IndexFreeSpaceMapVacuum so that reclaimed
     * space is visible to the FSM compaction.
     */
    VarBlockVacuumCleanup(rel);

    /*
     * Like btree/gist/gin: repair the MAIN fork FSM tree (including after VarBlock
     * RecordPageWithFreeSpace when upper layers can lag behind leaf slot usage).
     */
    IndexFreeSpaceMapVacuum(rel);

    /* stats is NULL if ambulkdelete not called */
    /* OK to return NULL if index not changed */
    if (stats == NULL) {
        return NULL;
    }

    stats->num_pages = RelationGetNumberOfBlocks(rel);

    return stats;
}