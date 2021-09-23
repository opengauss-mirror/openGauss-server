/* -------------------------------------------------------------------------
 *
 * spgxlog.cpp
 *	  WAL replay logic for SP-GiST
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			 src/gausskernel/storage/access/spgist/spgxlog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/spgist_private.h"
#include "utils/rel_gs.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "storage/standby.h"
#include "utils/memutils.h"
#include "access/multi_redo_api.h"

/*
 * Prepare a dummy SpGistState, with just the minimum info needed for replay.
 *
 * At present, all we need is enough info to support spgFormDeadTuple(),
 * plus the isBuild flag.
 */
void fillFakeState(SpGistState *state, const spgxlogState &stateSrc)
{
    errno_t rc = memset_s(state, sizeof(*state), 0, sizeof(*state));
    securec_check(rc, "\0", "\0");

    state->myXid = stateSrc.myXid;
    state->isBuild = stateSrc.isBuild;
    state->deadTupleStorage = (char *)palloc0(SGDTSIZE);
}

/*
 * Add a leaf tuple, or replace an existing placeholder tuple.	This is used
 * to replay SpGistPageAddNewItem() operations.  If the offset points at an
 * existing tuple, it had better be a placeholder tuple.
 */
void addOrReplaceTuple(Page page, Item tuple, int size, OffsetNumber offset)
{
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (offset <= maxoff) {
        SpGistDeadTuple dt = (SpGistDeadTuple)PageGetItem(page, PageGetItemId(page, offset));

        if (dt->tupstate != SPGIST_PLACEHOLDER)
            ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("SPGiST tuple to be replaced is not a placeholder")));

        Assert(SpGistPageGetOpaque(page)->nPlaceholder > 0);
        SpGistPageGetOpaque(page)->nPlaceholder--;

        PageIndexTupleDelete(page, offset);
    }

    Assert(offset <= maxoff + 1);

    if (PageAddItem(page, tuple, size, offset, false, false) != offset)
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add item of size %d to SPGiST index page", size)));
}

static void spgRedoCreateIndex(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    XLogInitBufferForRedo(record, 0, &buffer);
    spgRedoCreateIndexOperatorMetaPage(&buffer);
    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    XLogInitBufferForRedo(record, 1, &buffer);
    spgRedoCreateIndexOperatorRootPage(&buffer);
    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    XLogInitBufferForRedo(record, 2, &buffer);
    spgRedoCreateIndexOperatorLeafPage(&buffer);
    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);
}

static void spgRedoAddLeaf(XLogReaderState *record)
{
    char *ptr = XLogRecGetData(record);
    spgxlogAddLeaf *xldata = (spgxlogAddLeaf *)ptr;
    RedoBufferInfo buffer;
    XLogRedoAction action;

    /*
     * In normal operation we would have both current and parent pages locked
     * simultaneously; but in WAL replay it should be safe to update the leaf
     * page before updating the parent.
     */
    if (xldata->newPage) {
        XLogInitBufferForRedo(record, 0, &buffer);
        action = BLK_NEEDS_REDO;
    } else
        action = XLogReadBufferForRedo(record, 0, &buffer);
    if (action == BLK_NEEDS_REDO) {
        spgRedoAddLeafOperatorPage(&buffer, (void *)ptr);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }

    /* update parent downlink if necessary */
    if (xldata->offnumParent != InvalidOffsetNumber) {
        if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO) {
            BlockNumber blknoLeaf;
            XLogRecGetBlockTag(record, 0, NULL, NULL, &blknoLeaf);
            spgRedoAddLeafOperatorParent(&buffer, (void *)ptr, blknoLeaf);

            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }
}

static void spgRedoMoveLeafs(XLogReaderState *record)
{
    char *ptr = XLogRecGetData(record);
    spgxlogMoveLeafs *xldata = (spgxlogMoveLeafs *)ptr;
    OffsetNumber *toDelete = NULL;
    OffsetNumber *toInsert = NULL;
    int nInsert;
    RedoBufferInfo buffer;
    XLogRedoAction action;
    BlockNumber blknoDst;

    XLogRecGetBlockTag(record, 1, NULL, NULL, &blknoDst);

    nInsert = xldata->replaceDead ? 1 : xldata->nMoves + 1;

    ptr += SizeOfSpgxlogMoveLeafs;
    toDelete = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * xldata->nMoves;
    toInsert = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * nInsert;

    /* now ptr points to the list of leaf tuples
     *
     * In normal operation we would have all three pages (source, dest, and
     * parent) locked simultaneously; but in WAL replay it should be safe to
     * update them one at a time, as long as we do it in the right order.
     *
     * Insert tuples on the dest page (do first, so redirect is valid)
     */
    if (xldata->newPage) {
        XLogInitBufferForRedo(record, 1, &buffer);
        action = BLK_NEEDS_REDO;
    } else
        action = XLogReadBufferForRedo(record, 1, &buffer);

    if (action == BLK_NEEDS_REDO) {
        spgRedoMoveLeafsOpratorDstPage(&buffer, (void *)xldata, (void *)toInsert, (void *)ptr);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);

    /* Delete tuples from the source page, inserting a redirection pointer */
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        spgRedoMoveLeafsOpratorSrcPage(&buffer, (void *)xldata, (void *)toInsert, (void *)toDelete, blknoDst, nInsert);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);

    /* And update the parent downlink */
    if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO) {
        spgRedoMoveLeafsOpratorParentPage(&buffer, (void *)xldata, (void *)toInsert, blknoDst, nInsert);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void spgRedoAddNode(XLogReaderState *record)
{
    char *ptr = XLogRecGetData(record);
    spgxlogAddNode *xldata = (spgxlogAddNode *)ptr;
    char *innerTuple = NULL;
    SpGistInnerTupleData innerTupleHdr;
    RedoBufferInfo buffer;
    XLogRedoAction action;
    errno_t ret = EOK;

    /* we assume this is adequately aligned */
    ptr += sizeof(spgxlogAddNode);
    innerTuple = ptr;
    /* the tuple is unaligned, so make a copy to access its header */
    ret = memcpy_s(&innerTupleHdr, sizeof(SpGistInnerTupleData), innerTuple, sizeof(SpGistInnerTupleData));
    securec_check(ret, "", "");

    if (!XLogRecHasBlockRef(record, 1)) {
        /* update in place */
        Assert(xldata->parentBlk == -1);
        if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
            spgRedoAddNodeUpdateSrcPage(&buffer, (void *)xldata, (void *)innerTuple, (void *)&innerTupleHdr);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    } else {
        BlockNumber blknoNew;
        XLogRecGetBlockTag(record, 1, NULL, NULL, &blknoNew);

        /*
         * In normal operation we would have all three pages (source, dest,
         * and parent) locked simultaneously; but in WAL replay it should be
         * safe to update them one at a time, as long as we do it in the right
         * order. We must insert the new tuple before replacing the old tuple
         * with the redirect tuple.
         *
         * Install new tuple first so redirect is valid
         */
        if (xldata->newPage) {
            XLogInitBufferForRedo(record, 1, &buffer);
            action = BLK_NEEDS_REDO;
        } else
            action = XLogReadBufferForRedo(record, 1, &buffer);

        if (action == BLK_NEEDS_REDO) {
            spgRedoAddNodeOperatorDestPage(&buffer, (void *)xldata, (void *)innerTuple, (void *)&innerTupleHdr,
                                           blknoNew);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }

        /* Delete old tuple, replacing it with redirect or placeholder tuple */
        if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
            spgRedoAddNodeOperatorSrcPage(&buffer, (void *)xldata, blknoNew);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }

        /*
         * Update parent downlink (if we didn't do it as part of the source or
         * destination page update already).
         */
        if (xldata->parentBlk == 2) {
            if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO) {
                spgRedoAddNodeOperatorParentPage(&buffer, (void *)xldata, blknoNew);
                MarkBufferDirty(buffer.buf);
            }
            if (BufferIsValid(buffer.buf)) {
                UnlockReleaseBuffer(buffer.buf);
            }
        }
    }
}

static void spgRedoSplitTuple(XLogReaderState *record)
{
    char *ptr = XLogRecGetData(record);
    spgxlogSplitTuple *xldata = (spgxlogSplitTuple *)ptr;
    SpGistInnerTuple prefixTuple;
    SpGistInnerTuple postfixTuple;
    RedoBufferInfo buffer;
    XLogRedoAction action;

    /* we assume this is adequately aligned */
    ptr += sizeof(spgxlogSplitTuple);
    prefixTuple = (SpGistInnerTuple)ptr;
    ptr += prefixTuple->size;
    postfixTuple = (SpGistInnerTuple)ptr;

    /*
     * In normal operation we would have both pages locked simultaneously; but
     * in WAL replay it should be safe to update them one at a time, as long
     * as we do it in the right order.
     *
     * insert postfix tuple first to avoid dangling link
     */
    if (!xldata->postfixBlkSame) {
        if (xldata->newPage) {
            XLogInitBufferForRedo(record, 1, &buffer);
            action = BLK_NEEDS_REDO;
        } else
            action = XLogReadBufferForRedo(record, 1, &buffer);

        if (action == BLK_NEEDS_REDO) {
            spgRedoSplitTupleOperatorDestPage(&buffer, (void *)xldata, (void *)postfixTuple);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }

    /* now handle the original page */
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        spgRedoSplitTupleOperatorSrcPage(&buffer, (void *)xldata, (void *)prefixTuple, (void *)postfixTuple);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void spgRedoPickSplit(XLogReaderState *record)
{
    char *ptr = XLogRecGetData(record);
    spgxlogPickSplit *xldata = (spgxlogPickSplit *)ptr;
    char *innerTuple = NULL;
    SpGistInnerTupleData innerTupleHdr;
    OffsetNumber *toDelete = NULL;
    OffsetNumber *toInsert = NULL;
    uint8 *leafPageSelect = NULL;
    RedoBufferInfo srcBuffer;
    RedoBufferInfo destBuffer;
    RedoBufferInfo innerBuffer;
    BlockNumber blknoInner;
    XLogRedoAction action;
    errno_t ret = EOK;
    XLogRedoAction redoaction;

    XLogRecGetBlockTag(record, 2, NULL, NULL, &blknoInner);

    ptr += SizeOfSpgxlogPickSplit;
    toDelete = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * xldata->nDelete;
    toInsert = (OffsetNumber *)ptr;
    ptr += sizeof(OffsetNumber) * xldata->nInsert;
    leafPageSelect = (uint8 *)ptr;
    ptr += sizeof(uint8) * xldata->nInsert;

    innerTuple = ptr;
    /* the inner tuple is unaligned, so make a copy to access its header */
    ret = memcpy_s(&innerTupleHdr, sizeof(SpGistInnerTupleData), innerTuple, sizeof(SpGistInnerTupleData));
    securec_check(ret, "", "");
    ptr += innerTupleHdr.size;

    /* now ptr points to the list of leaf tuples */
    if (xldata->isRootSplit) {
        /* when splitting root, we touch it only in the guise of new inner */
        srcBuffer.buf = InvalidBuffer;
    } else {
        if (xldata->initSrc) {
            /* just re-init the source page */
            XLogInitBufferForRedo(record, 0, &srcBuffer);
            redoaction = BLK_NEEDS_REDO;
        } else {
            /*
             * Delete the specified tuples from source page.  (In case we're in
             * Hot Standby, we need to hold lock on the page till we're done
             * inserting leaf tuples and the new inner tuple, else the added
             * redirect tuple will be a dangling link.)
             */
            redoaction = XLogReadBufferForRedo(record, 0, &srcBuffer);
        }

        if (redoaction == BLK_NEEDS_REDO) {
            spgRedoPickSplitOperatorSrcPage(&srcBuffer, (void *)xldata, (void *)toDelete, blknoInner,
                                            (void *)leafPageSelect, (void *)toInsert);
        }
    }

    /* try to access dest page if any */
    if (!XLogRecHasBlockRef(record, 1)) {
        destBuffer.buf = InvalidBuffer;
    } else {
        if (xldata->initDest) {
            /* just re-init the dest page */
            XLogInitBufferForRedo(record, 1, &destBuffer);
            redoaction = BLK_NEEDS_REDO;
        } else {
            redoaction = XLogReadBufferForRedo(record, 1, &destBuffer);
        }

        if (redoaction == BLK_NEEDS_REDO) {
            spgRedoPickSplitOperatorDestPage(&destBuffer, (void *)xldata, (void *)leafPageSelect, (void *)toInsert);
        }

        /* don't update LSN etc till we're done with it */
    }

    /* Now update src and dest page LSNs if needed */
    if (BufferIsValid(srcBuffer.buf)) {
        MarkBufferDirty(srcBuffer.buf);
    }
    if (BufferIsValid(destBuffer.buf)) {
        MarkBufferDirty(destBuffer.buf);
    }

    /* restore new inner tuple */
    if (xldata->initInner) {
        XLogInitBufferForRedo(record, 2, &innerBuffer);
        action = BLK_NEEDS_REDO;
    } else
        action = XLogReadBufferForRedo(record, 2, &innerBuffer);

    if (action == BLK_NEEDS_REDO) {
        spgRedoPickSplitOperatorInnerPage(&innerBuffer, (void *)xldata, (void *)innerTuple, (void *)&innerTupleHdr,
                                          blknoInner);
        MarkBufferDirty(innerBuffer.buf);
    }
    if (BufferIsValid(innerBuffer.buf)) {
        UnlockReleaseBuffer(innerBuffer.buf);
    }

    /*
     * Now we can release the leaf-page locks.	It's okay to do this before
     * updating the parent downlink.
     */
    if (BufferIsValid(srcBuffer.buf))
        UnlockReleaseBuffer(srcBuffer.buf);
    if (BufferIsValid(destBuffer.buf))
        UnlockReleaseBuffer(destBuffer.buf);

    /* update parent downlink, unless we did it above */
    if (XLogRecHasBlockRef(record, 3)) {
        RedoBufferInfo parentBuffer;

        if (XLogReadBufferForRedo(record, 3, &parentBuffer) == BLK_NEEDS_REDO) {
            spgRedoPickSplitOperatorParentPage(&parentBuffer, (void *)xldata, blknoInner);
            MarkBufferDirty(parentBuffer.buf);
        }
        if (BufferIsValid(parentBuffer.buf)) {
            UnlockReleaseBuffer(parentBuffer.buf);
        }
    } else
        Assert(xldata->innerIsParent || xldata->isRootSplit);
}

static void spgRedoVacuumLeaf(XLogReaderState *record)
{
    char *ptr = XLogRecGetData(record);
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        spgRedoVacuumLeafOperatorPage(&buffer, (void *)ptr);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void spgRedoVacuumRoot(XLogReaderState *record)
{
    char *ptr = XLogRecGetData(record);
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        spgRedoVacuumRootOperatorPage(&buffer, (void *)ptr);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

bool IsSpgistVacuum(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (XLogRecGetRmid(record) == RM_SPGIST_ID) {
        if ((info == XLOG_SPGIST_VACUUM_LEAF) || (info == XLOG_SPGIST_VACUUM_ROOT) ||
            (info == XLOG_SPGIST_VACUUM_REDIRECT)) {
            return true;
        }
    }

    return false;
}

static void spgRedoVacuumRedirect(XLogReaderState *record)
{
    char *ptr = XLogRecGetData(record);
    RedoBufferInfo buffer;

    /*
     * If any redirection tuples are being removed, make sure there are no
     * live Hot Standby transactions that might need to see them.
     */
    if (InHotStandby && g_supportHotStandby) {
        spgxlogVacuumRedirect *xldata = (spgxlogVacuumRedirect *)ptr;
        if (TransactionIdIsValid(xldata->newestRedirectXid)) {
            RelFileNode node;

            XLogRecGetBlockTag(record, 0, &node, NULL, NULL);
            XLogRecPtr lsn = record->EndRecPtr;
            ResolveRecoveryConflictWithSnapshot(xldata->newestRedirectXid, node, lsn);
        }
    }

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        spgRedoVacuumRedirectOperatorPage(&buffer, (void *)ptr);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

void spg_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    MemoryContext oldCxt;

    oldCxt = MemoryContextSwitchTo(t_thrd.xlog_cxt.spg_opCtx);
    switch (info) {
        case XLOG_SPGIST_CREATE_INDEX:
            spgRedoCreateIndex(record);
            break;
        case XLOG_SPGIST_ADD_LEAF:
            spgRedoAddLeaf(record);
            break;
        case XLOG_SPGIST_MOVE_LEAFS:
            spgRedoMoveLeafs(record);
            break;
        case XLOG_SPGIST_ADD_NODE:
            spgRedoAddNode(record);
            break;
        case XLOG_SPGIST_SPLIT_TUPLE:
            spgRedoSplitTuple(record);
            break;
        case XLOG_SPGIST_PICKSPLIT:
            spgRedoPickSplit(record);
            break;
        case XLOG_SPGIST_VACUUM_LEAF:
            spgRedoVacuumLeaf(record);
            break;
        case XLOG_SPGIST_VACUUM_ROOT:
            spgRedoVacuumRoot(record);
            break;
        case XLOG_SPGIST_VACUUM_REDIRECT:
            spgRedoVacuumRedirect(record);
            break;
        default:
            ereport(PANIC, (errmsg("spg_redo: unknown op code %u", (uint32)info)));
    }

    (void)MemoryContextSwitchTo(oldCxt);
    MemoryContextReset(t_thrd.xlog_cxt.spg_opCtx);
}

void spg_xlog_startup(void)
{
    t_thrd.xlog_cxt.spg_opCtx = AllocSetContextCreate(CurrentMemoryContext, "SP-GiST temporary context",
                                                      ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                      ALLOCSET_DEFAULT_MAXSIZE);
}

void spg_xlog_cleanup(void)
{
    if (t_thrd.xlog_cxt.spg_opCtx != NULL) {
        MemoryContextDelete(t_thrd.xlog_cxt.spg_opCtx);
        t_thrd.xlog_cxt.spg_opCtx = NULL;
    }
}
