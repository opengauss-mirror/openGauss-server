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
 * bm25build.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25build.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <pthread.h>
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "storage/buf/block.h"
#include "utils/memutils.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "utils/builtins.h"
#include "access/datavec/bm25.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

/*
 * Initialize the build state
 */
static void InitBM25BuildState(BM25BuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;

    buildstate->reltuples = 0;
    buildstate->indtuples = 0;

    /* Get support functions */
    buildstate->procinfo = nullptr;
    buildstate->collation = index->rd_indcollation[0];

    buildstate->tmpCtx =
        AllocSetContextCreate(CurrentMemoryContext, "bm25 build temporary context", ALLOCSET_DEFAULT_SIZES);
}

static void FreeBuildState(BM25BuildState *buildstate)
{
    MemoryContextDelete(buildstate->tmpCtx);
}

static void AllocateForwardIdxForToken(Relation index, uint32 tokenCount, uint64 *start, uint64 *end, BM25DocForwardMetaPage metaPage)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    /* first page */
    if (metaPage->capacity == 0) {
        buf = BM25NewBuffer(index, MAIN_FORKNUM);
        BM25InitRegisterPage(index, &buf, &page, &state);
        BlockNumber newblk = BufferGetBlockNumber(buf);
        metaPage->startPage = newblk;
        metaPage->lastPage = newblk;
        metaPage->size = 0;
        metaPage->capacity = BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
        BM25CommitBuffer(buf, state);
    }
    /* need expand new page */
    if (metaPage->capacity - metaPage->size < tokenCount) {
        buf = ReadBuffer(index, metaPage->lastPage);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
        /* start append */
        while (metaPage->capacity - metaPage->size < tokenCount) {
            BM25AppendPage(index, &buf, &page, &state, MAIN_FORKNUM);
            BlockNumber newblk = BufferGetBlockNumber(buf);
            metaPage->lastPage = newblk;
            metaPage->capacity += BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
        }
        BM25CommitBuffer(buf, state);
    }
    *start = metaPage->size;
    *end = metaPage->size + tokenCount - 1;
    metaPage->size += tokenCount;
}

static BlockNumber SeekForwardBlknoForToken(Relation index, BlockNumber startBlkno, BlockNumber step)
{
    Buffer buf;
    Page page;
    BlockNumber curBlkno = startBlkno;
    for (int i = 0; i < step; ++i) {
        buf = ReadBuffer(index, curBlkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        curBlkno = BM25PageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
    }
    return curBlkno;
}

static void InsertDocForwardItem(Relation index, uint32 docId, BM25TokenizedDocData &tokenizedDoc, BM25EntryPages &bm25EntryPages
    , uint64 *forwardStart, uint64 *forwardEnd)
{
    Buffer buf;
    Page page;
    BlockNumber forwardStartBlkno;
    GenericXLogState *state;
    Buffer metabuf;
    Page metapage;
    BM25DocForwardMetaPage metaForwardPage;

    /* open forward list */
    metabuf = ReadBuffer(index, bm25EntryPages.docForwardPage);
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
    metaForwardPage = BM25PageGetDocForwardMeta(metapage);
    AllocateForwardIdxForToken(index, tokenizedDoc.tokenCount, forwardStart, forwardEnd, metaForwardPage);
    forwardStartBlkno = metaForwardPage->startPage;
    BM25CommitBuffer(metabuf, state);

    uint64 tokenIdx = *forwardStart;
    BlockNumber curStep = tokenIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
    BlockNumber preStep = curStep;
    BlockNumber curBlkno = SeekForwardBlknoForToken(index, forwardStartBlkno, curStep);
    uint16 offset;

    buf = ReadBuffer(index, curBlkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    for (int i = 0; i < tokenizedDoc.tokenCount; i++) {
        curStep = tokenIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
        offset = tokenIdx % BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
        if (curStep != preStep) {
            curBlkno = BM25PageGetOpaque(page)->nextblkno;
            BM25CommitBuffer(buf, state);

            buf = ReadBuffer(index, curBlkno);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            state = GenericXLogStart(index);
            page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
            preStep = curStep;
        }
        BM25DocForwardItem *forwardItem =
            (BM25DocForwardItem*)((char *)page + sizeof(PageHeaderData) + offset * BM25_DOCUMENT_FORWARD_ITEM_SIZE);
        forwardItem->tokenId = tokenizedDoc.tokenDatas[i].tokenId;
        forwardItem->tokenHash = tokenizedDoc.tokenDatas[i].tokenId;
        tokenIdx++;
    }
    BM25CommitBuffer(buf, state);
}

static bool ExpandDocumentListCapacityIfNeed(Relation index, BM25DocMetaPage docMetaPage, uint32 docId)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    bool expanded = false;
    /* start doc page */
    if (docMetaPage->docCapacity == 0) {
        buf = BM25NewBuffer(index, MAIN_FORKNUM);
        BM25InitRegisterPage(index, &buf, &page, &state);
        BlockNumber newblk = BufferGetBlockNumber(buf);
        docMetaPage->startDocPage = newblk;
        docMetaPage->lastDocPage = newblk;
        docMetaPage->docCapacity = BM25_DOCUMENT_MAX_COUNT_IN_PAGE;
        BM25CommitBuffer(buf, state);
        expanded = true;
    }
    /* need expand new page */
    if (docMetaPage->docCapacity <= docId) {
        buf = ReadBuffer(index, docMetaPage->lastDocPage);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
        /* start append */
        while (docMetaPage->docCapacity <= docId) {
            BM25AppendPage(index, &buf, &page, &state, MAIN_FORKNUM);
            BlockNumber newblk = BufferGetBlockNumber(buf);
            docMetaPage->lastDocPage = newblk;
            docMetaPage->docCapacity += BM25_DOCUMENT_MAX_COUNT_IN_PAGE;
            expanded = true;
        }
        BM25CommitBuffer(buf, state);
    }
    return expanded;
}

static void InsertDocumentItem(Relation index, uint32 docId, BM25TokenizedDocData &tokenizedDoc, ItemPointerData &ctid,
    BM25EntryPages &bm25EntryPages)
{
    Buffer buf;
    Page page;
    BlockNumber step;
    BlockNumber docRealBlkno;
    BlockNumber startDocPage;
    uint16 offset;
    GenericXLogState *state;
    Buffer metabuf;
    Page metapage;
    BM25DocMetaPage docMetaPage;
    uint64 forwardStart;
    uint64 forwardEnd;

    /* open document list */
    metabuf = ReadBuffer(index, bm25EntryPages.documentMetaPage);
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
    docMetaPage = BM25PageGetDocMeta(metapage);
    ExpandDocumentListCapacityIfNeed(index, docMetaPage, docId);
    startDocPage = docMetaPage->startDocPage;
    BM25CommitBuffer(metabuf, state);

    InsertDocForwardItem(index, docId, tokenizedDoc, bm25EntryPages, &forwardStart, &forwardEnd);

    /* write doc info into target blk */
    step = docId / BM25_DOCUMENT_MAX_COUNT_IN_PAGE;
    offset = docId % BM25_DOCUMENT_MAX_COUNT_IN_PAGE;
    docRealBlkno = SeekBlocknoForDoc(index, docId, startDocPage, step);
    buf = ReadBuffer(index, docRealBlkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    BM25DocumentItem *docItem =
        (BM25DocumentItem*)((char *)page + sizeof(PageHeaderData) + offset * BM25_DOCUMENT_ITEM_SIZE);
    unsigned short infomask = 0 | BM25_DOCUMENT_ITEM_SIZE;
    docItem->ctid.t_tid = ctid;
    docItem->ctid.t_info = infomask;
    docItem->docId = docId;
    docItem->docLength = tokenizedDoc.docLength;
    docItem->isActived = true;
    docItem->tokenStartIdx = forwardStart;
    docItem->tokenEndIdx = forwardEnd;
    BM25CommitBuffer(buf, state);
}

/*
 * Callback for building
 */
static void BM25BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values, const bool *isnull,
    bool tupleIsAlive, void *state)
{
    BM25BuildState *buildstate = (BM25BuildState *)state;
    MemoryContext oldCtx;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* insert document */
    BM25InsertDocument(index, values, hup->t_self, buildstate->bm25EntryPages);
    buildstate->indtuples++;

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

static bool BM25InsertDocument(Relation index, Datum *values, ItemPointerData &ctid, BM25EntryPages &bm25EntryPages)
{
    MemoryContext tempCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                         "temp bm25 index context",
                                                         ALLOCSET_DEFAULT_MINSIZE,
                                                         ALLOCSET_DEFAULT_INITSIZE,
                                                         ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldCtx = MemoryContextSwitchTo(tempCtx);
    /* todo tokenizer and insert doc */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(tempCtx);
    return true;
}

static BlockNumber CreateDocForwardMetaPage(Relation index, ForkNumber forkNum)
{
    Page page;
    GenericXLogState *state;
    BlockNumber metaBlkbo;
    BM25DocForwardMetaPage forwardMetaPage;

    // create matepage
    Buffer buf = BM25NewBuffer(index, forkNum);
    BM25InitRegisterPage(index, &buf, &page, &state);

    forwardMetaPage = BM25PageGetDocForwardMeta(page);
    forwardMetaPage->startPage = InvalidBlockNumber;
    forwardMetaPage->lastPage = InvalidBlockNumber;
    forwardMetaPage->size = 0;
    forwardMetaPage->capacity = 0;

    BM25PageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    BM25PageGetOpaque(page)->page_id = BM25_PAGE_ID;
    BM25PageGetOpaque(page)->unused = 0;

    ((PageHeader)page)->pd_lower = ((char *)forwardMetaPage + sizeof(BM25DocForwardMetaPageData)) - (char *)page;

    metaBlkbo = BufferGetBlockNumber(buf);
    BM25CommitBuffer(buf, state);
    return metaBlkbo;
}

static BM25EntryPages CreateEntryPages(Relation index, ForkNumber forkNum)
{
    BM25EntryPages entryPages;
    entryPages.documentMetaPage = CreateDocMetaPage(index, forkNum);
    entryPages.docForwardPage = CreateDocForwardMetaPage(index, forkNum);
    return entryPages;
}

/*
 * Create the metapage
 */
static void CreateMetaPage(Relation index, BM25BuildState *buildstate, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    BM25MetaPage metap;

    buf = BM25NewBuffer(index, forkNum);
    BM25InitRegisterPage(index, &buf, &page, &state);

    /* Set metapage data */
    metap = BM25PageGetMeta(page);
    metap->magicNumber = BM25_MAGIC_NUMBER;
    metap->version = BM25_VERSION;
    metap->entryPageList = CreateEntryPages(index, forkNum);
    buildstate->bm25EntryPages = metap->entryPageList;
    metap->documentCount = 0;
    metap->tokenCount = 0;
    metap->nextDocId = 0;
    metap->nextTokenId = 0;

    ((PageHeader)page)->pd_lower = ((char *)metap + sizeof(BM25MetaPageData)) - (char *)page;

    BM25CommitBuffer(buf, state);
}

/*
 * Build the index
 */
static void BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo, BM25BuildState *buildstate,
                       ForkNumber forkNum)
{
    InitBM25BuildState(buildstate, heap, index, indexInfo);
    CreateMetaPage(index, buildstate, forkNum);
    if (heap != NULL) {
        buildstate->reltuples = tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo, false,
            BM25BuildCallback, (void *)buildstate, NULL);
    }
    FreeBuildState(buildstate);
}

IndexBuildResult* bm25build_internal(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    BM25BuildState buildstate;

    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.indtuples;
    return result;
}

void bm25buildempty_internal(Relation index)
{
    IndexBuildResult *result;
    BM25BuildState buildstate;

    BuildIndex(NULL, index, NULL, &buildstate, MAIN_FORKNUM);
}

bool bm25insert_internal(Relation index, Datum *values, ItemPointer heapCtid)
{
    BM25MetaPageData meta;
    BM25GetMetaPageInfo(index, &meta);

    BM25InsertDocument(index, values, *heapCtid, meta.entryPageList);
    return true;
}