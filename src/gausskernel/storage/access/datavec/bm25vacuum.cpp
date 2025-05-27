
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

static void MarkDeleteDocuments(Relation index, IndexBulkDeleteCallback callback, void *callbackState, Vector<BM25DocumentItem> &deleteDocs,
    BM25EntryPages &entryPages, BufferAccessStrategy &bas)
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

static void FindDocumetTokens(Relation index, Vector<BM25DocumentItem> &deleteDocs, unordered_map<uint32, DeleteToken> deleteTokens,
    BM25EntryPages &entryPages, BufferAccessStrategy &bas)
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
            BM25DocForwardItem *forwardItem =
                (BM25DocForwardItem*)((char *)cpage + sizeof(PageHeaderData) + offset * BM25_DOCUMENT_FORWARD_ITEM_SIZE);
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

static void DeleteTokensFromPostinglist(Relation index, unordered_map<uint32, DeleteToken> deleteTokens, BM25EntryPages &entryPages)
{

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
    if (stats == NULL)
        return NULL;

    stats->num_pages = RelationGetNumberOfBlocks(rel);

    return stats;
}