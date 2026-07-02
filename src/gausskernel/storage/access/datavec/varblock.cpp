/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
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
 * varblock.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/varblock.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/generic_xlog.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/bufpage.h"
#include "storage/freespace.h"
#include "utils/rel.h"
#include "access/datavec/bm25.h"
#include "access/datavec/varblock.h"

/*
 * Variable-length chunk allocator:
 *
 * - Each chunk is one variable-length item: item = VarBlockChunkHeader + payload reserve.
 * - MAIN fork: prefer FSM (GetPageWithFreeSpace / RecordAndGetPageWithFreeSpace);
 *   on failure use last page / new page; call RecordPageWithFreeSpace after changing free space.
 * - Non-MAIN forks: still use last page / new page; do not touch FSM.
 * - VarBlockFreeChain: replace freed chunks with placeholders (PageIndexTupleDelete + PageAddItem)
 *   to preserve offset numbers of other items on the same page; update FSM on MAIN fork.
 */

/* Bound chain walks to avoid infinite loops on corruption; normal posting chains are far shorter */
#define VARBLOCK_MAX_CHAIN_LENGTH 10000000

/*
 * VarBlock is only used on BM25 inverted indexes. New pages use BM25NewBuffer() so P_NEW is
 * serialized on the BM25 lock page (parallel-safe).
 */

static inline Size VarBlockTargetSize(int level)
{
    if (level < 0) {
        level = 0;
    } else if (level >= VAR_BLOCK_LEVELS) {
        level = VAR_BLOCK_LEVELS - 1;
    }
    return VarBlockSize((uint8)level);
}

static void VarBlockFsmUpdateIfMain(Relation rel, ForkNumber forkNum, Buffer buf, bool building)
{
    if (forkNum != MAIN_FORKNUM) {
        return;
    }
    if (building) {
        /*
         * During CREATE INDEX build we need FSM updates to be immediately visible
         * to subsequent GetPageWithFreeSpace calls.
         *
         * Record the leaf (exact heap blk category) and propagate to upper
         * levels so GetPageWithFreeSpace can find reusable VarBlock pages
         * without waiting for vacuum.
         */
        BlockNumber blkno = BufferGetBlockNumber(buf);
        Size freespace = PageGetFreeSpace(BufferGetPage(buf));
        RecordPageWithFreeSpace(rel, blkno, freespace);
        UpdateFreeSpaceMap(rel, blkno, blkno, freespace, false);
    } else {
        RecordPageWithFreeSpace(rel, BufferGetBlockNumber(buf), PageGetFreeSpace(BufferGetPage(buf)));
    }
}

static inline VarBlockChunkHeader* VarBlockGetChunk(Page page, OffsetNumber offnum)
{
    ItemId id = PageGetItemId(page, offnum);
    return (VarBlockChunkHeader *)PageGetItem(page, id);
}

static bool VarBlockTryGetChunk(Page page, OffsetNumber offnum, VarBlockChunkHeader **hdr)
{
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (offnum < FirstOffsetNumber || offnum > maxoff) {
        return false;
    }

    ItemId id = PageGetItemId(page, offnum);
    if (!ItemIdIsUsed(id) || !ItemIdIsNormal(id)) {
        return false;
    }

    uint16 itemLen = ItemIdGetLength(id);
    if (itemLen < sizeof(VarBlockChunkHeader)) {
        return false;
    }

    *hdr = (VarBlockChunkHeader *)PageGetItem(page, id);

    /* Reject placeholder chunks (left behind by VarBlockFreeChain). */
    if (VarBlockIsPlaceholder(*hdr)) {
        return false;
    }

    return true;
}

static inline void VarBlockInitHeader(VarBlockChunkHeader *hdr, int level)
{
    ItemPointerSetInvalid(&hdr->next_ctid);
    hdr->payload_len = 0;
    hdr->level = (uint8)level;
}

static inline void VarBlockMakePlaceholder(VarBlockChunkHeader *hdr)
{
    ItemPointerSetInvalid(&hdr->next_ctid);
    hdr->payload_len = 0;
    hdr->level = VAR_BLOCK_LEVEL_PLACEHOLDER;
}

/*
 * Only reuse pages that already look like VarBlock pages.
 * This prevents FSM from returning BM25 metadata/data pages
 * (e.g. docForwardBlknoTable pages) that happen to have free space.
 *
 * Empty pages (no line pointers) must not be treated as compatible: IndexFreeSpaceMapVacuum
 * can register empty free-list / misc BM25 pages on the FSM; VarBlock items must not share
 * those pages with BM25FreeDocumentItem layout. Reject empty pages here so allocation falls
 * back to BM25NewBuffer rather than mixing layouts on a page that only "looks" free.
 */
static bool VarBlockPageCompatible(Page page)
{
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (maxoff == InvalidOffsetNumber) {
        return false;
    }
    for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
        ItemId iid = PageGetItemId(page, off);
        if (!ItemIdIsUsed(iid) || !ItemIdIsNormal(iid)) {
            return false;
        }
        uint16 len = ItemIdGetLength(iid);
        if (len != VAR_BLOCK_PLACEHOLDER_SIZE &&
            len != VAR_BLOCK_LEVEL0_SIZE && len != VAR_BLOCK_LEVEL1_SIZE &&
            len != VAR_BLOCK_LEVEL2_SIZE && len != VAR_BLOCK_LEVEL3_SIZE &&
            len != VAR_BLOCK_LEVEL4_SIZE) {
            return false;
        }
    }
    return true;
}

/*
 * Allocate one chunk of the given level on this page.
 *
 * Caller must hold BUFFER_LOCK_EXCLUSIVE and register the page in state/page.
 */
static OffsetNumber VarBlockAllocOnPage(Buffer buf, Page page, int level, GenericXLogState *state)
{
    Size targetSize = VarBlockTargetSize(level);
    if (PageGetFreeSpace(page) < targetSize) {
        return InvalidOffsetNumber;
    }
    /*
     * PageAddItem copies the item data into the page.
     * We only need a placeholder and header init, so a zero-filled buffer suffices.
     */
    Item zeroItem = (Item)palloc0(targetSize);
    OffsetNumber offnum = PageAddItem(page, zeroItem, targetSize, InvalidOffsetNumber, false, false);
    if (offnum == InvalidOffsetNumber) {
        pfree(zeroItem);
        if (state != NULL) {
            GenericXLogAbort(state);
        }
        UnlockReleaseBuffer(buf);
        elog(ERROR, "failed to add varblock item");
    }
    pfree(zeroItem);

    VarBlockChunkHeader *hdr = VarBlockGetChunk(page, offnum);
    VarBlockInitHeader(hdr, level);
    return offnum;
}

/* First pass: walk chain and count links (same stop conditions as fill pass). */
static int VarBlockCountChainLinks(Relation rel, ForkNumber forkNum, const ItemPointerData *head_ctid)
{
    int n = 0;
    ItemPointerData cur = *head_ctid;

    while (ItemPointerIsValid(&cur)) {
        if (++n > VARBLOCK_MAX_CHAIN_LENGTH) {
            ereport(ERROR, (errmsg("VarBlock chain exceeds maximum length (possible cycle or corruption)")));
        }
        BlockNumber blkno = ItemPointerGetBlockNumber(&cur);
        OffsetNumber offno = ItemPointerGetOffsetNumber(&cur);
        Buffer buf = ReadBufferExtended(rel, forkNum, blkno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);
        VarBlockChunkHeader *hdr = NULL;
        if (!VarBlockTryGetChunk(page, offno, &hdr)) {
            UnlockReleaseBuffer(buf);
            ereport(WARNING, (errmsg("VarBlock chain corrupted at (%u,%u), stop collect", blkno, offno)));
            break;
        }
        cur = hdr->next_ctid;
        UnlockReleaseBuffer(buf);
    }
    return n;
}

/*
 * Second pass: copy up to n ctids into arr; returns number of entries written (may be < n if corrupt).
 */
static int VarBlockFillChainCtids(Relation rel, ForkNumber forkNum, const ItemPointerData *head_ctid,
    ItemPointerData *arr, int n)
{
    ItemPointerData cur = *head_ctid;
    int filled = 0;

    while (filled < n && ItemPointerIsValid(&cur)) {
        arr[filled++] = cur;
        BlockNumber blkno = ItemPointerGetBlockNumber(&cur);
        OffsetNumber offno = ItemPointerGetOffsetNumber(&cur);
        Buffer buf = ReadBufferExtended(rel, forkNum, blkno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);
        VarBlockChunkHeader *hdr = NULL;
        if (!VarBlockTryGetChunk(page, offno, &hdr)) {
            UnlockReleaseBuffer(buf);
            ereport(WARNING, (errmsg("VarBlock chain corrupted at (%u,%u), stop collect", blkno, offno)));
            break;
        }
        cur = hdr->next_ctid;
        UnlockReleaseBuffer(buf);
    }
    return filled;
}

/*
 * Walk the CTID chain and write each chunk ctid into *out_arr (caller pfree's).
 * Returns chunk count; returns 0 with *out_arr NULL if the chain is empty or invalid.
 */
static int VarBlockCollectChainCtids(Relation rel, ForkNumber forkNum, const ItemPointerData *head_ctid,
    ItemPointerData **out_arr)
{
    *out_arr = NULL;
    if (head_ctid == NULL || !ItemPointerIsValid(head_ctid)) {
        return 0;
    }

    int n = VarBlockCountChainLinks(rel, forkNum, head_ctid);
    if (n == 0) {
        return 0;
    }

    ItemPointerData *arr = (ItemPointerData *)palloc(sizeof(ItemPointerData) * n);
    int filled = VarBlockFillChainCtids(rel, forkNum, head_ctid, arr, n);
    if (filled != n) {
        /* Corrupted chain: keep valid prefix so callers can rebuild metadata. */
        n = filled;
    }
    if (n <= 0) {
        pfree(arr);
        *out_arr = NULL;
        return 0;
    }

    *out_arr = arr;
    return n;
}

/* Locked FSM hit page + allocation params (bundled for G.FUD.05) */
typedef struct VarBlockFsmHitCtx {
    Relation rel;
    ForkNumber forkNum;
    Buffer buf;
    Page page;
    int level;
    bool building;
} VarBlockFsmHitCtx;

/*
 * FSM candidate fits: allocate chunk on the already-EXCLUSIVE-locked page, update FSM,
 * set ctid, release buffer. Caller returns immediately after this.
 */
static void VarBlockAllocAndFinishFsmHit(VarBlockFsmHitCtx *ctx, ItemPointerData *ctid)
{
    OffsetNumber offnum;
    if (ctx->building) {
        offnum = VarBlockAllocOnPage(ctx->buf, ctx->page, ctx->level, NULL);
        MarkBufferDirty(ctx->buf);
    } else {
        GenericXLogState *state = GenericXLogStart(ctx->rel);
        ctx->page = GenericXLogRegisterBuffer(state, ctx->buf, GENERIC_XLOG_FULL_IMAGE);
        offnum = VarBlockAllocOnPage(ctx->buf, ctx->page, ctx->level, state);
        GenericXLogFinish(state);
    }
    VarBlockFsmUpdateIfMain(ctx->rel, ctx->forkNum, ctx->buf, ctx->building);
    ItemPointerSet(ctid, BufferGetBlockNumber(ctx->buf), offnum);
    UnlockReleaseBuffer(ctx->buf);
}

/*
 * FSM found no suitable page (or skipped): extend with a fresh page via BM25NewBuffer.
 */
static void VarBlockAllocNewChunkOnFreshPage(Relation rel, ForkNumber forkNum, int level, ItemPointerData *ctid,
    bool building)
{
    Buffer buf = BM25NewBuffer(rel, forkNum);
    GenericXLogState *state = NULL;
    OffsetNumber offnum;
    Page page;

    if (building) {
        page = BufferGetPage(buf);
        PageInit(page, BufferGetPageSize(buf), 0);
        offnum = VarBlockAllocOnPage(buf, page, level, NULL);
        MarkBufferDirty(buf);
    } else {
        state = GenericXLogStart(rel);
        page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
        PageInit(page, BufferGetPageSize(buf), 0);
        offnum = VarBlockAllocOnPage(buf, page, level, state);
        GenericXLogFinish(state);
    }
    VarBlockFsmUpdateIfMain(rel, forkNum, buf, building);
    ItemPointerSet(ctid, BufferGetBlockNumber(buf), offnum);
    UnlockReleaseBuffer(buf);
}

/*
 * Pick a page for a new chunk:
 *  - MAIN fork: FSM lookup, re-check under lock; else RecordAndGetPageWithFreeSpace;
 *    if still none, last page / extend.
 *  - Other forks: last page / extend.
 */
static void VarBlockAllocNewChunk(Relation rel, ForkNumber forkNum, int level, ItemPointerData *ctid, bool building)
{
    Buffer buf = InvalidBuffer;
    Page page = NULL;
    Size need = VarBlockTargetSize(level);
    /*
     * freespace.c: fsm_space_needed_to_cat ERRORs when needed > MaxFSMRequestSize.
     * Same as heap: skip FSM for oversized requests; go straight to last page / extend.
     */
    if (forkNum == MAIN_FORKNUM && need <= MaxFSMRequestSize) {
        BlockNumber fsmBlk = GetPageWithFreeSpace(rel, need);
        while (BlockNumberIsValid(fsmBlk)) {
            buf = ReadBufferExtended(rel, forkNum, fsmBlk, RBM_NORMAL, NULL);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            page = BufferGetPage(buf);
            Size avail = PageGetFreeSpace(page);
            bool compatible = VarBlockPageCompatible(page);
            if (compatible && avail >= need) {
                VarBlockFsmHitCtx hit = { rel, forkNum, buf, page, level, building };
                VarBlockAllocAndFinishFsmHit(&hit, ctid);
                return;
            }
            if (!compatible) {
                avail = 0;
            }
            fsmBlk = RecordAndGetPageWithFreeSpace(rel, fsmBlk, avail, need);
            UnlockReleaseBuffer(buf);
            buf = InvalidBuffer;
        }
    }
    /* Upper layers may hold EXCLUSIVE on the relation tail; always allocate via BM25NewBuffer. */
    VarBlockAllocNewChunkOnFreshPage(rel, forkNum, level, ctid, building);
}

ItemPointerData VarBlockAllocFirstChunk(Relation rel, ForkNumber forkNum, int level_hint, bool building)
{
    ItemPointerData ctid;
    ItemPointerSetInvalid(&ctid);

    int level = level_hint;
    if (level < 0) {
        level = 0;
    } else if (level >= VAR_BLOCK_LEVELS) {
        level = 0;
    }

    VarBlockAllocNewChunk(rel, forkNum, level, &ctid, building);
    return ctid;
}

ItemPointerData VarBlockExtendChain(Relation rel, ForkNumber forkNum, const ItemPointerData *tail_ctid, bool building)
{
    ItemPointerData new_ctid;
    ItemPointerSetInvalid(&new_ctid);

    if (!ItemPointerIsValid(tail_ctid)) {
        return new_ctid;
    }

    /*
     * To avoid list corruption from next_ctid write timing combined with
     * "new page / reuse last page" paths, use two steps:
     * 1) Read tail level, call VarBlockAllocNewChunk for the new chunk;
     * 2) In a separate WAL transaction, update the tail's next_ctid.
     */
    BlockNumber tailBlk = ItemPointerGetBlockNumber(tail_ctid);
    OffsetNumber tailOff = ItemPointerGetOffsetNumber(tail_ctid);

    Buffer tailBuf = ReadBufferExtended(rel, forkNum, tailBlk, RBM_NORMAL, NULL);
    LockBuffer(tailBuf, BUFFER_LOCK_EXCLUSIVE);

    Page tailPage = BufferGetPage(tailBuf);
    VarBlockChunkHeader *tailHdr = VarBlockGetChunk(tailPage, tailOff);
    int curLevel = tailHdr->level;
    int nextLevel = curLevel + 1;
    if (nextLevel >= VAR_BLOCK_LEVELS) {
        nextLevel = VAR_BLOCK_LEVELS - 1;
    }
    UnlockReleaseBuffer(tailBuf);

    /* Allocate new chunk first; get new tail ctid */
    VarBlockAllocNewChunk(rel, forkNum, nextLevel, &new_ctid, building);

    /*
     * Then set tail next_ctid to the new chunk.
     * Re-lock here so we do not hold tailBuf across VarBlockAllocNewChunk (double-lock /
     * deadlock risk) and so page layout cannot change under us during allocation.
     */
    tailBuf = ReadBufferExtended(rel, forkNum, tailBlk, RBM_NORMAL, NULL);
    LockBuffer(tailBuf, BUFFER_LOCK_EXCLUSIVE);
    if (building) {
        Page tailPage = BufferGetPage(tailBuf);
        VarBlockChunkHeader *walTailHdr = VarBlockGetChunk(tailPage, tailOff);
        walTailHdr->next_ctid = new_ctid;
        MarkBufferDirty(tailBuf);
    } else {
        GenericXLogState *state = GenericXLogStart(rel);
        Page walTailPage = GenericXLogRegisterBuffer(state, tailBuf, GENERIC_XLOG_FULL_IMAGE);
        VarBlockChunkHeader *walTailHdr = VarBlockGetChunk(walTailPage, tailOff);
        walTailHdr->next_ctid = new_ctid;
        GenericXLogFinish(state);
    }
    VarBlockFsmUpdateIfMain(rel, forkNum, tailBuf, building);
    UnlockReleaseBuffer(tailBuf);

    return new_ctid;
}

ItemPointerData VarBlockFindTailCtid(Relation rel, ForkNumber forkNum, const ItemPointerData *start_ctid)
{
    ItemPointerData invalid;
    ItemPointerSetInvalid(&invalid);

    if (start_ctid == NULL || !ItemPointerIsValid(start_ctid)) {
        return invalid;
    }

    ItemPointerData cur = *start_ctid;
    int nvisited = 0;

    while (ItemPointerIsValid(&cur)) {
        if (++nvisited > VARBLOCK_MAX_CHAIN_LENGTH) {
            ereport(ERROR,
                (errmsg("VarBlock chain exceeds maximum length while resolving tail (possible cycle or corruption)")));
        }

        BlockNumber blkno = ItemPointerGetBlockNumber(&cur);
        OffsetNumber offno = ItemPointerGetOffsetNumber(&cur);

        Buffer buf = ReadBufferExtended(rel, forkNum, blkno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);
        VarBlockChunkHeader *hdr = NULL;
        if (!VarBlockTryGetChunk(page, offno, &hdr)) {
            UnlockReleaseBuffer(buf);
            ereport(WARNING,
                (errmsg("VarBlock chain corrupted at (%u,%u) while resolving tail", blkno, offno)));
            return invalid;
        }
        ItemPointerData next = hdr->next_ctid;
        UnlockReleaseBuffer(buf);

        if (!ItemPointerIsValid(&next)) {
            return cur;
        }
        cur = next;
    }

    return invalid;
}

void VarBlockReadChain(Relation rel, ForkNumber forkNum, const ItemPointerData *head_ctid,
                  VarBlockReadCallback cb, VarBlockReadContext *cbArg)
{
    if (cb == NULL || head_ctid == NULL || !ItemPointerIsValid(head_ctid)) {
        return;
    }

    ItemPointerData cur = *head_ctid;
    int nvisited = 0;
    while (ItemPointerIsValid(&cur)) {
        if (++nvisited > VARBLOCK_MAX_CHAIN_LENGTH) {
            ereport(ERROR, (errmsg("VarBlock chain exceeds maximum length (possible cycle or corruption)")));
        }

        BlockNumber blkno = ItemPointerGetBlockNumber(&cur);
        OffsetNumber offno = ItemPointerGetOffsetNumber(&cur);

        Buffer buf = ReadBufferExtended(rel, forkNum, blkno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);
        VarBlockChunkHeader *hdr = NULL;
        if (!VarBlockTryGetChunk(page, offno, &hdr)) {
            UnlockReleaseBuffer(buf);
            ereport(WARNING, (errmsg("VarBlock chain corrupted at (%u,%u), stop read", blkno, offno)));
            break;
        }
        char *payload = ((char *)hdr) + sizeof(VarBlockChunkHeader);

        cb(hdr, payload, cbArg);

        ItemPointerData next = hdr->next_ctid;
        UnlockReleaseBuffer(buf);

        if (!ItemPointerIsValid(&next)) {
            break;
        }
        cur = next;
    }
}

void VarBlockFreeChain(Relation rel, ForkNumber forkNum, const ItemPointerData *head_ctid, bool building)
{
    ItemPointerData *ctids = NULL;
    int n;

    if (head_ctid == NULL || !ItemPointerIsValid(head_ctid)) {
        return;
    }

    n = VarBlockCollectChainCtids(rel, forkNum, head_ctid, &ctids);
    if (n <= 0 || ctids == NULL) {
        if (ctids != NULL) {
            pfree(ctids);
        }
        return;
    }

    /*
     * Build the placeholder chunk once.  We replace each freed chunk with
     * this minimal-size placeholder to preserve offset numbers for other
     * token chains on the same page.
     */
    VarBlockChunkHeader placeholder;
    VarBlockMakePlaceholder(&placeholder);

    for (int i = 0; i < n; i++) {
        if (i > 0 && ItemPointerEquals(&ctids[i], &ctids[i - 1])) {
            continue;
        }
        BlockNumber blkno = ItemPointerGetBlockNumber(&ctids[i]);
        OffsetNumber offno = ItemPointerGetOffsetNumber(&ctids[i]);

        Buffer buf = ReadBufferExtended(rel, forkNum, blkno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(buf);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        if (offno < FirstOffsetNumber || offno > maxoff) {
            UnlockReleaseBuffer(buf);
            ereport(WARNING, (errmsg("skip invalid VarBlock offnum (%u,%u) during free", blkno, offno)));
            continue;
        }
        ItemId iid = PageGetItemId(page, offno);
        if (!ItemIdIsUsed(iid) || !ItemIdIsNormal(iid)) {
            UnlockReleaseBuffer(buf);
            ereport(WARNING, (errmsg("skip non-normal VarBlock item (%u,%u) during free", blkno, offno)));
            continue;
        }

        /*
         * Replace the chunk with a placeholder to preserve offset numbers.
         *
         * PageIndexTupleDelete physically removes the item and compacts line
         * pointers, changing offset numbers of subsequent items.  PageAddItem
         * then inserts the placeholder at the same offset, restoring the
         * original offset numbers for other items on the page.
         *
         * Net effect: other token chains' ctids remain valid, and the freed
         * data space is reclaimed (large chunk -> small placeholder).
         */
        if (building) {
            PageIndexTupleDelete(page, offno);
            OffsetNumber newOff = PageAddItem(page, (Item)&placeholder,
                VAR_BLOCK_PLACEHOLDER_SIZE, offno, false, false);
            if (newOff != offno) {
                ereport(ERROR, (errmsg("VarBlockFreeChain: PageAddItem returned %u, expected %u", newOff, offno)));
            }
            MarkBufferDirty(buf);
        } else {
            GenericXLogState *state = GenericXLogStart(rel);
            Page walPage = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
            PageIndexTupleDelete(walPage, offno);
            OffsetNumber newOff = PageAddItem(walPage, (Item)&placeholder,
                VAR_BLOCK_PLACEHOLDER_SIZE, offno, false, false);
            if (newOff != offno) {
                GenericXLogAbort(state);
                ereport(ERROR, (errmsg("VarBlockFreeChain: PageAddItem returned %u, expected %u", newOff, offno)));
            }
            GenericXLogFinish(state);
        }
        VarBlockFsmUpdateIfMain(rel, forkNum, buf, building);

        UnlockReleaseBuffer(buf);
    }

    pfree(ctids);
}

/*
 * Vacuum cleanup: physically remove trailing placeholder chunks from VarBlock pages.
 *
 * Placeholder chunks are left behind by VarBlockFreeChain to preserve offset
 * numbers of other items on the same page.  Over time, placeholders accumulate
 * and waste line-pointer slots.  This function safely removes only those
 * placeholders at the end of a page, because deleting trailing items does not
 * change the offset numbers of preceding items.
 *
 * Reference: SP-GiST vacuumRedirectAndPlaceholder (spgvacuum.cpp).
 */
void VarBlockVacuumCleanup(Relation rel)
{
    BlockNumber numBlks = RelationGetNumberOfBlocks(rel);

    for (BlockNumber blkno = 0; blkno < numBlks; blkno++) {
        Buffer buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(buf);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

        /*
         * Scan backwards to find trailing placeholder chunks.
         * Only placeholders at the very end of the page can be safely removed,
         * because PageIndexMultiDelete compacts line pointers and would change
         * offset numbers of non-placeholder items that follow the deleted ones.
         */
        OffsetNumber firstTrailingPlaceholder = InvalidOffsetNumber;

        for (OffsetNumber off = maxoff; off >= FirstOffsetNumber; off = OffsetNumberPrev(off)) {
            ItemId iid = PageGetItemId(page, off);
            if (!ItemIdIsUsed(iid) || !ItemIdIsNormal(iid)) {
                break;
            }
            uint16 len = ItemIdGetLength(iid);
            if (len != VAR_BLOCK_PLACEHOLDER_SIZE) {
                break;
            }
            VarBlockChunkHeader *hdr = (VarBlockChunkHeader *)PageGetItem(page, iid);
            if (!VarBlockIsPlaceholder(hdr)) {
                break;
            }
            firstTrailingPlaceholder = off;
        }

        if (firstTrailingPlaceholder == InvalidOffsetNumber) {
            UnlockReleaseBuffer(buf);
            continue;
        }

        /* Collect trailing placeholder offset numbers. */
        int nDead = maxoff - firstTrailingPlaceholder + 1;
        OffsetNumber *itemnos = (OffsetNumber *)palloc(sizeof(OffsetNumber) * nDead);
        for (int i = 0; i < nDead; i++) {
            itemnos[i] = firstTrailingPlaceholder + i;
        }

        /* Physically delete trailing placeholders (safe: no non-placeholder follows). */
        GenericXLogState *state = GenericXLogStart(rel);
        Page walPage = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
        PageIndexMultiDelete(walPage, itemnos, nDead);
        GenericXLogFinish(state);

        pfree(itemnos);

        /* Update FSM so the reclaimed space is visible for future allocations. */
        VarBlockFsmUpdateIfMain(rel, MAIN_FORKNUM, buf, false);

        UnlockReleaseBuffer(buf);
    }
}
