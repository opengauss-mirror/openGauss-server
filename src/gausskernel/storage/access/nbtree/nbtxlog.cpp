/* -------------------------------------------------------------------------
 *
 * nbtxlog.cpp
 *	  WAL replay logic for btrees.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.	  
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/nbtree/nbtxlog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"

#include "storage/procarray.h"
#include "storage/smgr/relfilenode.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "access/multi_redo_api.h"
#include "access/parallel_recovery/dispatcher.h"

#ifdef ENABLE_UT
#define static
#endif

/*
 * We must keep track of expected insertions due to page splits, and apply
 * them manually if they are not seen in the WAL log during replay.  This
 * makes it safe for page insertion to be a multiple-WAL-action process.
 *
 * Similarly, deletion of an only child page and deletion of its parent page
 * form multiple WAL log entries, and we have to be prepared to follow through
 * with the deletion if the log ends between.
 *
 * The data structure is a simple linked list --- this should be good enough,
 * since we don't expect a page split or multi deletion to remain incomplete
 * for long.  In any case we need to respect the order of operations.
 */
typedef struct bt_incomplete_action {
    RelFileNode node; /* the index */
    bool is_split;    /* T = pending split, F = pending delete */
    /* these fields are for a split: */
    bool is_root;         /* we split the root */
    BlockNumber leftblk;  /* left half of split */
    BlockNumber rightblk; /* right half of split */
    XLogPhyBlock leftpblk; /* left physical block */
    XLogPhyBlock rightpblk; /* right physical block */
    uint32 level;
    /* these fields are for a delete: */
    BlockNumber delblk; /* parent block to be deleted */
    XLogPhyBlock delpblk; /* parent physical block */
} bt_incomplete_action;

static void log_incomplete_split(const RelFileNode *node, BlockNumber leftblk, BlockNumber rightblk,
    const XLogPhyBlock &leftpblk, const XLogPhyBlock &rightpblk, bool is_root)
{
    MemoryContext oldCtx = NULL;
    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
    }

    bt_incomplete_action *action = (bt_incomplete_action *)palloc(sizeof(bt_incomplete_action));
    if (log_min_messages <= DEBUG4) {
        ereport(LOG, (errmsg("[BTREE_ACTION_TRACE]log_incomplete_split: spc:%u,db:%u,rel:%u,"
                             "leftblk:%u,rightblk:%u,is_root:%d",
                             node->spcNode, node->dbNode, node->relNode, leftblk, rightblk, is_root)));
    }
    action->node = *node;
    action->is_split = true;
    action->is_root = is_root;
    action->leftblk = leftblk;
    action->rightblk = rightblk;
    action->leftpblk = leftpblk;
    action->rightpblk = rightpblk;
    t_thrd.xlog_cxt.incomplete_actions = lappend(t_thrd.xlog_cxt.incomplete_actions, action);

    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        (void)MemoryContextSwitchTo(oldCtx);
    }
}

static void forget_matching_split(const RelFileNode *node, BlockNumber downlink, bool is_root)
{
    ListCell *l = NULL;
    if (log_min_messages <= DEBUG4) {
        ereport(LOG, (errmsg("[BTREE_ACTION_TRACE]forget_matching_split begin: spc:%u,db:%u,rel:%u,"
                             "downlink:%u, is_root:%d",
                             node->spcNode, node->dbNode, node->relNode, downlink, is_root)));
    }
    MemoryContext oldCtx = NULL;
    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
    }
    foreach (l, t_thrd.xlog_cxt.incomplete_actions) {
        bt_incomplete_action *action = (bt_incomplete_action *)lfirst(l);

        if (RelFileNodeEquals(*node, action->node) && action->is_split && downlink == action->rightblk) {
            if (log_min_messages <= DEBUG4) {
                ereport(LOG,
                        (errmsg("[BTREE_ACTION_TRACE]forget_matching_split successfully: input spc:%u,db:%u,rel:%u,"
                                "downlink:%u, is_root:%d, action spc:%u,db:%u,rel:%u,"
                                "is_split:%d,is_root:%d,leftblk:%u,rightblk:%u,level:%u,delblk:%u",
                                node->spcNode, node->dbNode, node->relNode, downlink, is_root, action->node.spcNode,
                                action->node.dbNode, action->node.relNode, action->is_split, action->is_root,
                                action->leftblk, action->rightblk, action->level, action->delblk)));
            }
            if (is_root != action->is_root)
                ereport(LOG, (errmsg("forget_matching_split: fishy is_root data (expected %d, got %d)", action->is_root,
                                     is_root)));
            t_thrd.xlog_cxt.incomplete_actions = list_delete_ptr(t_thrd.xlog_cxt.incomplete_actions, action);
            pfree(action);
            break; /* need not look further */
        }
    }
    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        (void)MemoryContextSwitchTo(oldCtx);
    }
}

static void log_incomplete_deletion(const RelFileNode *node, BlockNumber delblk, const XLogPhyBlock &delpblk)
{
    MemoryContext oldCtx = NULL;
    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
    }
    bt_incomplete_action *action = (bt_incomplete_action *)palloc(sizeof(bt_incomplete_action));
    if (log_min_messages <= DEBUG4) {
        ereport(LOG, (errmsg("[BTREE_ACTION_TRACE]log_incomplete_deletion: spc:%u,db:%u,rel:%u,"
                             "delblk:%u",
                             node->spcNode, node->dbNode, node->relNode, delblk)));
    }
    action->node = *node;
    action->is_split = false;
    action->delblk = delblk;
    action->delpblk = delpblk;
    t_thrd.xlog_cxt.incomplete_actions = lappend(t_thrd.xlog_cxt.incomplete_actions, action);
    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        (void)MemoryContextSwitchTo(oldCtx);
    }
}

static void forget_matching_deletion(const RelFileNode *node, BlockNumber delblk)
{
    ListCell *l = NULL;

    MemoryContext oldCtx = NULL;
    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
    }

    if (SHOW_DEBUG_MESSAGE()) {
        ereport(LOG,
            (errmsg("[BTREE_ACTION_TRACE]forget_matching_deletion begin: spc:%u,db:%u,rel:%u,"
                    "delblk:%u",
                node->spcNode,
                node->dbNode,
                node->relNode,
                delblk)));
   }

    foreach (l, t_thrd.xlog_cxt.incomplete_actions) {
        bt_incomplete_action *action = (bt_incomplete_action *)lfirst(l);

        if (RelFileNodeEquals(*node, action->node) && !action->is_split && delblk == action->delblk) {
            if (SHOW_DEBUG_MESSAGE()) {
                ereport(LOG,
                        (errmsg("[BTREE_ACTION_TRACE]forget_matching_deletion successfully: input spc:%u,db:%u,rel:%u,"
                                "delblk:%u, action spc:%u,db:%u,rel:%u,"
                                "is_split:%d,is_root:%d,leftblk:%u,rightblk:%u,level:%u,delblk:%u",
                                node->spcNode, node->dbNode, node->relNode, delblk, action->node.spcNode,
                                action->node.dbNode, action->node.relNode, action->is_split, action->is_root,
                                action->leftblk, action->rightblk, action->level, action->delblk)));
            }

            t_thrd.xlog_cxt.incomplete_actions = list_delete_ptr(t_thrd.xlog_cxt.incomplete_actions, action);
            pfree(action);
            break; /* need not look further */
        }
    }
    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        (void)MemoryContextSwitchTo(oldCtx);
    }
}

static void _bt_restore_meta(XLogReaderState *record, uint8 block_id)
{
    RedoBufferInfo metabuf;
    char *ptr = NULL;
    Size len;

    XLogInitBufferForRedo(record, block_id, &metabuf);
    ptr = XLogRecGetBlockData(record, block_id, &len);

    BtreeRestoreMetaOperatorPage(&metabuf, (void *)ptr, len);
    MarkBufferDirty(metabuf.buf);
    UnlockReleaseBuffer(metabuf.buf);
}

/*
 * _bt_clear_incomplete_split -- clear INCOMPLETE_SPLIT flag on a page
 *
 * This is a common subroutine of the redo functions of all the WAL record
 * types that can insert a downlink: insert, split, and newroot.
 */
static void _bt_clear_incomplete_split(XLogReaderState *record, uint8 block_id)
{
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, block_id, &buffer) == BLK_NEEDS_REDO) {
        BtreeXlogClearIncompleteSplit(&buffer);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void btree_xlog_insert(bool isleaf, bool ismeta, XLogReaderState *record, bool issplitupgrade)
{
    xl_btree_insert *xlrec = (xl_btree_insert *)XLogRecGetData(record);
    RelFileNode rnode;
    RedoBufferInfo buffer;
    char *datapos = NULL;
    BlockNumber downlink = 0;

    /*
     * Insertion to an internal page finishes an incomplete split at the child
     * level.  Clear the incomplete-split flag in the child.  Note: during
     * normal operation, the child and parent pages are locked at the same
     * time, so that clearing the flag and inserting the downlink appear
     * atomic to other backends.  We don't bother with that during replay,
     * because readers don't care about the incomplete-split flag and there
     * cannot be updates happening.
     */
    if (!issplitupgrade) {
        XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);
        if (!isleaf) {
            datapos = (char *)xlrec + SizeOfBtreeInsert;
            errno_t rc = memcpy_s(&downlink, sizeof(BlockNumber), datapos, sizeof(BlockNumber));
            securec_check(rc, "\0", "\0");
        }
    } else {
        if (!isleaf) {
            _bt_clear_incomplete_split(record, BTREE_INSERT_CHILD_BLOCK_NUM);
        }
    }

    if (XLogReadBufferForRedo(record, BTREE_INSERT_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        Size datalen;

        datapos = XLogRecGetBlockData(record, BTREE_INSERT_ORIG_BLOCK_NUM, &datalen);
        BtreeXlogInsertOperatorPage(&buffer, (void *)xlrec, (void *)datapos, datalen);
        MarkBufferDirty(buffer.buf);
    }

    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }

    /*
     * Note: in normal operation, we'd update the metapage while still holding
     * lock on the page we inserted into.  But during replay it's not
     * necessary to hold that lock, since no other index updates can be
     * happening concurrently, and readers will cope fine with following an
     * obsolete link from the metapage.
     */
    if (!issplitupgrade) {
        if (ismeta) {
            _bt_restore_meta(record, 1);
        }
        /* Forget any split this insertion completes */
        if (!isleaf) {
            forget_matching_split(&rnode, downlink, false);
        }
    } else {
        if (ismeta) {
            _bt_restore_meta(record, BTREE_INSERT_META_BLOCK_NUM);
        }
    }
}

static void btree_xlog_split_update(bool onleft, bool isroot, XLogReaderState *record)
{
    Size datalen;
    char *datapos = NULL;
    RelFileNode rnode;
    BlockNumber leftsib;
    BlockNumber rightsib;
    BlockNumber rnext;

    XLogRecGetBlockTag(record, BTREE_SPLIT_LEFT_BLOCK_NUM, &rnode, NULL, &leftsib);
    XLogRecGetBlockTag(record, BTREE_SPLIT_RIGHT_BLOCK_NUM, NULL, NULL, &rightsib);
    if (!XLogRecGetBlockTag(record, BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM, NULL, NULL, &rnext)) {
        rnext = P_NONE;
    }

    xl_btree_split *xlrec = (xl_btree_split *)XLogRecGetData(record);
    bool isleaf = (xlrec->level == 0);

    if (!isleaf) {
        _bt_clear_incomplete_split(record, BTREE_SPLIT_CHILD_BLOCK_NUM);
    }

    /* Reconstruct right (new) sibling page from scratch */
    RedoBufferInfo rbuf;
    XLogInitBufferForRedo(record, BTREE_SPLIT_RIGHT_BLOCK_NUM, &rbuf);

    datapos = XLogRecGetBlockData(record, BTREE_SPLIT_RIGHT_BLOCK_NUM, &datalen);
    BtreeXlogSplitOperatorRightpage(&rbuf, (void *)xlrec, leftsib, rnext, (void *)datapos, datalen);
    MarkBufferDirty(rbuf.buf);

    RedoBufferInfo lbuf;
    if (XLogReadBufferForRedo(record, BTREE_SPLIT_LEFT_BLOCK_NUM, &lbuf) == BLK_NEEDS_REDO) {
        datapos = XLogRecGetBlockData(record, BTREE_SPLIT_LEFT_BLOCK_NUM, &datalen);
        BtreeXlogSplitOperatorLeftpage(&lbuf, (void *)xlrec, rightsib, onleft, (void *)datapos, datalen);
        MarkBufferDirty(lbuf.buf);
    }

    if (BufferIsValid(lbuf.buf)) {
        UnlockReleaseBuffer(lbuf.buf);
    }
    UnlockReleaseBuffer(rbuf.buf);

    if (rnext != P_NONE) {
        RedoBufferInfo buffer;

        if (XLogReadBufferForRedo(record, BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
            BtreeXlogSplitOperatorNextpage(&buffer, rightsib);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }
}

static void btree_xlog_split(bool onleft, bool isroot, XLogReaderState *record, bool issplitupgrade)
{
    if (issplitupgrade) {
        btree_xlog_split_update(onleft, isroot, record);
        return;
    }

    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_split *xlrec = (xl_btree_split *)XLogRecGetData(record);
    bool isleaf = (xlrec->level == 0);
    RedoBufferInfo lbuf;
    RedoBufferInfo rbuf;
    Page rpage;
    BTPageOpaqueInternal ropaque;
    char *datapos = NULL;
    Size datalen;
    Item left_hikey = NULL;
    Size left_hikeysz = 0;
    RelFileNode rnode;
    XLogPhyBlock leftpblk;
    XLogPhyBlock rightpblk;
    BlockNumber leftsib;
    BlockNumber rightsib;
    BlockNumber rnext;

    XLogRecGetBlockTag(record, 0, &rnode, NULL, &leftsib, &leftpblk);
    XLogRecGetBlockTag(record, 1, NULL, NULL, &rightsib, &rightpblk);
    if (!XLogRecGetBlockTag(record, 2, NULL, NULL, &rnext)) {
        rnext = P_NONE;
    }

    /* Forget any split this insertion completes */
    if (!isleaf) {
        BlockNumber downlink;
        /* we assume SizeOfBtreeSplit is at least 16-bit aligned */
        datapos = (char *)xlrec + SizeOfBtreeSplit;
        downlink = BlockIdGetBlockNumber((BlockId)datapos);
        forget_matching_split(&rnode, downlink, false);
    }

    /* Reconstruct right (new) sibling page from scratch */
    XLogInitBufferForRedo(record, 1, &rbuf);
    datapos = XLogRecGetBlockData(record, 1, &datalen);
    rpage = rbuf.pageinfo.page;

    _bt_pageinit(rpage, rbuf.pageinfo.pagesize);
    ropaque = (BTPageOpaqueInternal)PageGetSpecialPointer(rpage);

    ropaque->btpo_prev = leftsib;
    ropaque->btpo_next = rnext;
    ropaque->btpo.level = xlrec->level;
    ropaque->btpo_flags = isleaf ? BTP_LEAF : 0;
    ropaque->btpo_cycleid = 0;

    _bt_restore_page(rpage, datapos, (int)datalen);

    /*
     * On leaf level, the high key of the left page is equal to the first key
     * on the right page.
     */
    if (isleaf) {
        ItemId hiItemId = PageGetItemId(rpage, P_FIRSTDATAKEY(ropaque));

        left_hikey = PageGetItem(rpage, hiItemId);
        left_hikeysz = ItemIdGetLength(hiItemId);
    }

    PageSetLSN(rpage, lsn);
    MarkBufferDirty(rbuf.buf);
    /* don't release the buffer yet; we touch right page's first item below
     * Now reconstruct left (original) sibling page
     */
    if (XLogReadBufferForRedo(record, 0, &lbuf) == BLK_NEEDS_REDO) {
        /*
         * To retain the same physical order of the tuples that they had, we
         * initialize a temporary empty page for the left page and add all the
         * items to that in item number order.  This mirrors how _bt_split()
         * works.  It's not strictly required to retain the same physical
         * order, as long as the items are in the correct item number order,
         * but it helps debugging.  See also _bt_restore_page(), which does
         * the same for the right page.
         */
        Page lpage = lbuf.pageinfo.page;
        BTPageOpaqueInternal lopaque = (BTPageOpaqueInternal)PageGetSpecialPointer(lpage);
        OffsetNumber off;
        Item newitem = NULL;
        Size newitemsz = 0;
        Page newlpage;
        OffsetNumber leftoff;

        datapos = XLogRecGetBlockData(record, 0, &datalen);

        /* Extract left hikey and its size (assuming 16-bit alignment) */

        if (!isleaf) {
            left_hikey = (Item)datapos;
            left_hikeysz = MAXALIGN(IndexTupleSize(left_hikey));
            datapos += left_hikeysz;
            datalen -= left_hikeysz;
        }

        if (onleft) {
            newitem = (Item)datapos;
            newitemsz = MAXALIGN(IndexTupleSize(newitem));
            datapos += newitemsz;
            datalen -= newitemsz;
        }

        Assert(datalen == 0);

        /* assure that memory is properly allocated, prevent from core dump caused by buffer unpin */
        START_CRIT_SECTION();
        newlpage = PageGetTempPageCopySpecial(lpage);
        END_CRIT_SECTION();

        /* Set high key */
        leftoff = P_HIKEY;
        if (PageAddItem(newlpage, left_hikey, left_hikeysz, P_HIKEY, false, false) == InvalidOffsetNumber)
            ereport(PANIC, (errmsg("failed to add high key to left page after split")));
        leftoff = OffsetNumberNext(leftoff);

        for (off = P_FIRSTDATAKEY(lopaque); off < xlrec->firstright; off++) {
            ItemId itemid;
            Size itemsz;
            Item item;

            /* add the new item if it was inserted on left page */
            if (onleft && off == xlrec->newitemoff) {
                if (PageAddItem(newlpage, newitem, newitemsz, leftoff, false, false) == InvalidOffsetNumber)
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                                    errmsg("failed to add new item to left page after split")));
                leftoff = OffsetNumberNext(leftoff);
            }

            itemid = PageGetItemId(lpage, off);
            itemsz = ItemIdGetLength(itemid);
            item = PageGetItem(lpage, itemid);
            if (PageAddItem(newlpage, item, itemsz, leftoff, false, false) == InvalidOffsetNumber)
                ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add old item to left page after split")));
            leftoff = OffsetNumberNext(leftoff);
        }

        /* cope with possibility that newitem goes at the end */
        if (onleft && off == xlrec->newitemoff) {
            if (PageAddItem(newlpage, newitem, newitemsz, leftoff, false, false) == InvalidOffsetNumber)
                ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add new item to left page after split")));
            leftoff = OffsetNumberNext(leftoff);
        }

        PageRestoreTempPage(newlpage, lpage);

        /* Fix opaque fields */
        lopaque = (BTPageOpaqueInternal)PageGetSpecialPointer(lpage);
        lopaque->btpo_flags = isleaf ? BTP_LEAF : 0;
        lopaque->btpo_next = rightsib;
        lopaque->btpo_cycleid = 0;

        PageSetLSN(lpage, lsn);
        MarkBufferDirty(lbuf.buf);
    }

    /* We no longer need the buffers */
    if (BufferIsValid(lbuf.buf)) {
        UnlockReleaseBuffer(lbuf.buf);
    }
    UnlockReleaseBuffer(rbuf.buf);

    /*
     * Fix left-link of the page to the right of the new right sibling.
     *
     * Note: in normal operation, we do this while still holding lock on the
     * two split pages.  However, that's not necessary for correctness in WAL
     * replay, because no other index update can be in progress, and readers
     * will cope properly when following an obsolete left-link.
     */
    if (rnext != P_NONE) {
        RedoBufferInfo buffer;

        if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO) {
            Page page = buffer.pageinfo.page;
            BTPageOpaqueInternal pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);

            pageop->btpo_prev = rightsib;

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }

    log_incomplete_split(&rnode, leftsib, rightsib, leftpblk, rightpblk, isroot);
}

static void btree_xlog_vacuum(XLogReaderState *record)
{
    xl_btree_vacuum *xlrec = (xl_btree_vacuum *)XLogRecGetData(record);
    RedoBufferInfo redobuf;

    /*
     * If queries might be active then we need to ensure every leaf page is
     * unpinned between the lastBlockVacuumed and the current block, if there
     * are any.  This prevents replay of the VACUUM from reaching the stage of
     * removing heap tuples while there could still be indexscans "in flight"
     * to those particular tuples (see nbtree/README).
     *
     * It might be worth checking if there are actually any backends running;
     * if not, we could just skip this.
     *
     * Since VACUUM can visit leaf pages out-of-order, it might issue records
     * with lastBlockVacuumed >= block; that's not an error, it just means
     * nothing to do now.
     *
     * Note: since we touch all pages in the range, we will lock non-leaf
     * pages, and also any empty (all-zero) pages that may be in the index. It
     * doesn't seem worth the complexity to avoid that.  But it's important
     * that HotStandbyActiveInReplay() will not return true if the database
     * isn't yet consistent; so we need not fear reading still-corrupt blocks
     * here during crash recovery.
     */
    if (HotStandbyActive() && IS_SINGLE_NODE) {
        RelFileNode thisrnode;
        BlockNumber thisblkno;
        BlockNumber blkno;

        XLogRecGetBlockTag(record, BTREE_VACUUM_ORIG_BLOCK_NUM, &thisrnode, NULL, &thisblkno);

        for (blkno = xlrec->lastBlockVacuumed + 1; blkno < thisblkno && !IsSegmentFileNode(thisrnode); blkno++) {
            /*
             * We use RBM_NORMAL_NO_LOG mode because it's not an error
             * condition to see all-zero pages.  The original btvacuumpage
             * scan would have skipped over all-zero pages, noting them in FSM
             * but not bothering to initialize them just yet; so we mustn't
             * throw an error here.  (We could skip acquiring the cleanup lock
             * if PageIsNew, but it's probably not worth the cycles to test.)
             *
             * XXX we don't actually need to read the block, we just need to
             * confirm it is unpinned. If we had a special call into the
             * buffer manager we could optimise this so that if the block is
             * not in shared_buffers we confirm it as unpinned.
             */
            Buffer buffer = XLogReadBufferExtended(thisrnode, MAIN_FORKNUM, blkno, RBM_NORMAL_NO_LOG, NULL);
            if (BufferIsValid(buffer)) {
                LockBufferForCleanup(buffer);
                UnlockReleaseBuffer(buffer);
            }
        }
    }

    /*
     * Like in btvacuumpage(), we need to take a cleanup lock on every leaf
     * page. See nbtree/README for details.
     */
    if (XLogReadBufferForRedoExtended(record, BTREE_VACUUM_ORIG_BLOCK_NUM, RBM_NORMAL, true, &redobuf) ==
        BLK_NEEDS_REDO) {
        char *ptr = NULL;
        Size len;

        ptr = XLogRecGetBlockData(record, BTREE_VACUUM_ORIG_BLOCK_NUM, &len);
        BtreeXlogVacuumOperatorPage(&redobuf, (void *)xlrec, (void *)ptr, len);
        MarkBufferDirty(redobuf.buf);
    }
    if (BufferIsValid(redobuf.buf))
        UnlockReleaseBuffer(redobuf.buf);
}

static void btree_xlog_delete(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    /*
     * If we have any conflict processing to do, it must happen before we
     * update the page.
     *
     * Btree delete records can conflict with standby queries.  You might
     * think that vacuum records would conflict as well, but we've handled
     * that already.  XLOG_HEAP2_CLEANUP_INFO records provide the highest xid
     * cleaned by the vacuum of the heap and so we can resolve any conflicts
     * just once when that arrives.  After that we know that no conflicts
     * exist from individual btree vacuum records on that index.
     *
     * XXX: In MPPDB, we don't support hot_standby query on standby.
     */
    if (XLogReadBufferForRedo(record, BTREE_DELETE_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        BtreeXlogDeleteOperatorPage(&buffer, (void *)XLogRecGetData(record), XLogRecGetDataLen(record));

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void btree_xlog_delete_page(uint8 info, XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_delete_page *xlrec = (xl_btree_delete_page *)XLogRecGetData(record);
    RelFileNode rnode;
    BlockNumber parent;
    BlockNumber target;
    BlockNumber leftsib;
    BlockNumber rightsib;
    XLogPhyBlock pblk;
    RedoBufferInfo buffer;
    Page page;
    BTPageOpaqueInternal pageop;

    leftsib = xlrec->leftblk;
    rightsib = xlrec->rightblk;

    XLogRecGetBlockTag(record, 0, &rnode, NULL, &target);
    XLogRecGetBlockTag(record, 3, NULL, NULL, &parent, &pblk);

    /*
     * In normal operation, we would lock all the pages this WAL record
     * touches before changing any of them.  In WAL replay, it should be okay
     * to lock just one page at a time, since no concurrent index updates can
     * be happening, and readers should not care whether they arrive at the
     * target page or not (since it's surely empty).
     *
     * parent page
     */
    if (XLogReadBufferForRedo(record, 3, &buffer) == BLK_NEEDS_REDO) {
        OffsetNumber poffset, maxoff;

        page = buffer.pageinfo.page;

        pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
        poffset = xlrec->poffset;
        maxoff = PageGetMaxOffsetNumber(page);
        if (poffset >= maxoff) {
            Assert(info == XLOG_BTREE_MARK_PAGE_HALFDEAD);
            Assert(poffset == P_FIRSTDATAKEY(pageop));
            PageIndexTupleDelete(page, poffset);
            pageop->btpo_flags |= BTP_HALF_DEAD;
        } else {
            ItemId itemid;
            IndexTuple itup;
            OffsetNumber nextoffset;

            Assert(info != XLOG_BTREE_MARK_PAGE_HALFDEAD);
            itemid = PageGetItemId(page, poffset);
            itup = (IndexTuple)PageGetItem(page, itemid);
            ItemPointerSet(&(itup->t_tid), rightsib, P_HIKEY);
            nextoffset = OffsetNumberNext(poffset);
            PageIndexTupleDelete(page, nextoffset);
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);

    /* Fix left-link of right sibling */
    if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO) {
        page = buffer.pageinfo.page;

        pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
        pageop->btpo_prev = leftsib;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);

    /* Fix right-link of left sibling, if any */
    if (leftsib != P_NONE) {
        if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO) {
            page = buffer.pageinfo.page;

            pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
            pageop->btpo_next = rightsib;

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf))
            UnlockReleaseBuffer(buffer.buf);
    }

    /* Rewrite target page as empty deleted page */
    XLogInitBufferForRedo(record, 0, &buffer);
    page = buffer.pageinfo.page;

    _bt_pageinit(page, buffer.pageinfo.pagesize);
    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);

    pageop->btpo_prev = leftsib;
    pageop->btpo_next = rightsib;
    pageop->btpo_flags = BTP_DELETED;
    pageop->btpo_cycleid = 0;
    ((BTPageOpaque)pageop)->xact = xlrec->btpo_xact;

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    /* Update metapage if needed */
    if (info == XLOG_BTREE_UNLINK_PAGE_META)
        _bt_restore_meta(record, 4);

    /* Forget any completed deletion */
    forget_matching_deletion(&rnode, target);

    /* If parent became half-dead, remember it for deletion */
    if (info == XLOG_BTREE_MARK_PAGE_HALFDEAD) {
        log_incomplete_deletion(&rnode, parent, pblk);
    }
}

static void btree_xlog_mark_page_halfdead(uint8 info, XLogReaderState *record)
{
    xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *)XLogRecGetData(record);
    RedoBufferInfo pbuffer;

    /*
     * In normal operation, we would lock all the pages this WAL record
     * touches before changing any of them.  In WAL replay, it should be okay
     * to lock just one page at a time, since no concurrent index updates can
     * be happening, and readers should not care whether they arrive at the
     * target page or not (since it's surely empty).
     */

    /* parent page */
    if (XLogReadBufferForRedo(record, BTREE_HALF_DEAD_PARENT_PAGE_NUM, &pbuffer) == BLK_NEEDS_REDO) {
        BtreeXlogHalfdeadPageOperatorParentpage(&pbuffer, xlrec);
        MarkBufferDirty(pbuffer.buf);
    }
    if (BufferIsValid(pbuffer.buf)) {
        UnlockReleaseBuffer(pbuffer.buf);
    }

    RedoBufferInfo lbuffer;
    XLogInitBufferForRedo(record, BTREE_HALF_DEAD_LEAF_PAGE_NUM, &lbuffer);
    BtreeXlogHalfdeadPageOperatorLeafpage(&lbuffer, xlrec);

    MarkBufferDirty(lbuffer.buf);
    UnlockReleaseBuffer(lbuffer.buf);
}

static void btree_xlog_unlink_page(uint8 info, XLogReaderState *record)
{
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)XLogRecGetData(record);
    BlockNumber leftsib;
    BlockNumber rightsib;

    leftsib = xlrec->leftsib;
    rightsib = xlrec->rightsib;

    /*
     * In normal operation, we would lock all the pages this WAL record
     * touches before changing any of them.  In WAL replay, it should be okay
     * to lock just one page at a time, since no concurrent index updates can
     * be happening, and readers should not care whether they arrive at the
     * target page or not (since it's surely empty).
     */

    /* Fix left-link of right sibling */
    RedoBufferInfo rbuffer;
    if (XLogReadBufferForRedo(record, BTREE_UNLINK_PAGE_RIGHT_NUM, &rbuffer) == BLK_NEEDS_REDO) {
        BtreeXlogUnlinkPageOperatorRightpage(&rbuffer, xlrec);
        MarkBufferDirty(rbuffer.buf);
    }
    if (BufferIsValid(rbuffer.buf)) {
        UnlockReleaseBuffer(rbuffer.buf);
    }

    /* Fix right-link of left sibling, if any */
    if (leftsib != P_NONE) {
        RedoBufferInfo lbuffer;
        if (XLogReadBufferForRedo(record, BTREE_UNLINK_PAGE_LEFT_NUM, &lbuffer) == BLK_NEEDS_REDO) {
            BtreeXlogUnlinkPageOperatorLeftpage(&lbuffer, xlrec);
            MarkBufferDirty(lbuffer.buf);
        }
        if (BufferIsValid(lbuffer.buf)) {
            UnlockReleaseBuffer(lbuffer.buf);
        }
    }

    /* Rewrite target page as empty deleted page */
    RedoBufferInfo buffer;
    XLogInitBufferForRedo(record, BTREE_UNLINK_PAGE_CUR_PAGE_NUM, &buffer);
    BtreeXlogUnlinkPageOperatorCurpage(&buffer, xlrec);

    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    /*
     * If we deleted a parent of the targeted leaf page, instead of the leaf
     * itself, update the leaf to point to the next remaining child in the
     * branch.
     */
    if (XLogRecHasBlockRef(record, BTREE_UNLINK_PAGE_CHILD_NUM)) {
        /*
         * There is no real data on the page, so we just re-create it from
         * scratch using the information from the WAL record.
         */
        RedoBufferInfo cbuffer;
        XLogInitBufferForRedo(record, BTREE_UNLINK_PAGE_CHILD_NUM, &cbuffer);
        BtreeXlogUnlinkPageOperatorChildpage(&cbuffer, xlrec);

        MarkBufferDirty(cbuffer.buf);
        UnlockReleaseBuffer(cbuffer.buf);
    }

    /* Update metapage if needed */
    if (info == XLOG_BTREE_UNLINK_PAGE_META) {
        _bt_restore_meta(record, BTREE_UNLINK_PAGE_META_NUM);
    }
}

static void btree_xlog_newroot_update(XLogReaderState *record)
{
    xl_btree_newroot *xlrec = (xl_btree_newroot *)XLogRecGetData(record);
    BlockNumber downlink = 0;
    RedoBufferInfo buffer;
    RedoBufferInfo lbuffer;
    char *ptr = NULL;
    Size len;

    XLogInitBufferForRedo(record, BTREE_NEWROOT_ORIG_BLOCK_NUM, &buffer);
    ptr = XLogRecGetBlockData(record, BTREE_NEWROOT_ORIG_BLOCK_NUM, &len);
    BtreeXlogNewrootOperatorPage(&buffer, (void *)xlrec, (void *)ptr, len, &downlink);

    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    lbuffer.buf = InvalidBuffer;
    if (xlrec->level > 0 && XLogReadBufferForRedo(record, BTREE_NEWROOT_LEFT_BLOCK_NUM, &lbuffer) == BLK_NEEDS_REDO) {
        BtreeXlogClearIncompleteSplit(&lbuffer);
        MarkBufferDirty(lbuffer.buf);
    }

    if (BufferIsValid(lbuffer.buf)) {
        UnlockReleaseBuffer(lbuffer.buf);
    }
    _bt_restore_meta(record, BTREE_NEWROOT_META_BLOCK_NUM);
}

static void btree_xlog_newroot(XLogReaderState *record, bool issplitupgrade)
{
    if (issplitupgrade) {
        btree_xlog_newroot_update(record);
        return;
    }

    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_newroot *xlrec = (xl_btree_newroot *)XLogRecGetData(record);
    RelFileNode rnode;
    Page page;
    BTPageOpaqueInternal pageop;
    BlockNumber downlink = 0;
    RedoBufferInfo buffer;
    char *ptr = NULL;
    Size len;

    XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);

    XLogInitBufferForRedo(record, 0, &buffer);
    page = buffer.pageinfo.page;

    _bt_pageinit(page, buffer.pageinfo.pagesize);
    pageop = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    pageop->btpo_flags = BTP_ROOT;
    pageop->btpo_prev = pageop->btpo_next = P_NONE;
    pageop->btpo.level = xlrec->level;
    if (xlrec->level == 0) {
        pageop->btpo_flags |= BTP_LEAF;
    }
    pageop->btpo_cycleid = 0;

    if (xlrec->level > 0) {
        IndexTuple itup;

        ptr = XLogRecGetBlockData(record, 0, &len);
        _bt_restore_page(page, ptr, len);

        /* extract downlink to the right-hand split page */
        itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, P_FIRSTKEY));
        downlink = ItemPointerGetBlockNumber(&(itup->t_tid));
        Assert(ItemPointerGetOffsetNumber(&(itup->t_tid)) == P_HIKEY);
    }

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    /* Check to see if this satisfies any incomplete insertions */
    if (xlrec->level > 0) {
        forget_matching_split(&rnode, downlink, true);
    }
    _bt_restore_meta(record, 1);
}

bool IsBtreeVacuum(const XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (XLogRecGetRmid(record) == RM_BTREE_ID) {
        if ((info == XLOG_BTREE_REUSE_PAGE) || (info == XLOG_BTREE_VACUUM) || (info == XLOG_BTREE_DELETE) ||
            (info == XLOG_BTREE_UNLINK_PAGE) || (info == XLOG_BTREE_UNLINK_PAGE_META) ||
            (info == XLOG_BTREE_MARK_PAGE_HALFDEAD)) {
            return true;
        }
    }

    return false;
}

static void btree_xlog_reuse_page(XLogReaderState *record)
{
    xl_btree_reuse_page *xlrec = (xl_btree_reuse_page *)XLogRecGetData(record);

    /*
     * Btree reuse_page records exist to provide a conflict point when we
     * reuse pages in the index via the FSM.  That's all they do though.
     *
     * latestRemovedXid was the page's btpo.xact.  The btpo.xact <
     * RecentGlobalXmin test in _bt_page_recyclable() conceptually mirrors the
     * pgxact->xmin > limitXmin test in GetConflictingVirtualXIDs().
     * Consequently, one XID value achieves the same exclusion effect on
     * master and standby.
     */
    RelFileNode tmp_node;
    RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));

    if (InHotStandby && g_supportHotStandby) {
        XLogRecPtr lsn = record->EndRecPtr;
        ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, tmp_node, lsn);
    }
}

void btree_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool issplitupgrade = (XLogRecGetInfo(record) & BTREE_SPLIT_UPGRADE_FLAG) != 0;
    bool isdelupgrade = (XLogRecGetInfo(record) & BTREE_DELETE_UPGRADE_FLAG) != 0;

    switch (info) {
        case XLOG_BTREE_INSERT_LEAF:
            btree_xlog_insert(true, false, record, issplitupgrade);
            break;
        case XLOG_BTREE_INSERT_UPPER:
            btree_xlog_insert(false, false, record, issplitupgrade);
            break;
        case XLOG_BTREE_INSERT_META:
            btree_xlog_insert(false, true, record, issplitupgrade);
            break;
        case XLOG_BTREE_SPLIT_L:
            btree_xlog_split(true, false, record, issplitupgrade);
            break;
        case XLOG_BTREE_SPLIT_R:
            btree_xlog_split(false, false, record, issplitupgrade);
            break;
        case XLOG_BTREE_SPLIT_L_ROOT:
            btree_xlog_split(true, true, record, issplitupgrade);
            break;
        case XLOG_BTREE_SPLIT_R_ROOT:
            btree_xlog_split(false, true, record, issplitupgrade);
            break;
        case XLOG_BTREE_VACUUM:
            btree_xlog_vacuum(record);
            break;
        case XLOG_BTREE_DELETE:
            btree_xlog_delete(record);
            break;
        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META:
            if (!isdelupgrade) {
                btree_xlog_delete_page(info, record);
            } else {
                btree_xlog_unlink_page(info, record);
            }
            break;
        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
            if (!isdelupgrade) {
                btree_xlog_delete_page(info, record);
            } else {
                btree_xlog_mark_page_halfdead(info, record);
            }
            break;
        case XLOG_BTREE_NEWROOT:
            btree_xlog_newroot(record, issplitupgrade);
            break;
        case XLOG_BTREE_REUSE_PAGE:
            btree_xlog_reuse_page(record);
            break;
        default:
            ereport(PANIC, (errmsg("btree_redo: unknown op code %hhu", info)));
    }
}

void btree_xlog_startup(void)
{
    t_thrd.xlog_cxt.incomplete_actions = NIL;
}

void btree_xlog_finish_incomplete_split(bt_incomplete_action *action)
{
    Buffer lbuf, rbuf;
    Page lpage, rpage;
    BTPageOpaqueInternal lpageop, rpageop;
    bool is_only = false;
    Relation reln;

    lbuf = XLogReadBufferExtended(action->node, MAIN_FORKNUM, action->leftblk, RBM_NORMAL, &action->leftpblk);
    /* failure is impossible because we wrote this page earlier */
    if (BufferIsValid(lbuf))
        LockBuffer(lbuf, BUFFER_LOCK_EXCLUSIVE);
    else
        ereport(PANIC, (errmsg("btree_xlog_cleanup: left block unfound")));
    lpage = (Page)BufferGetPage(lbuf);
    lpageop = (BTPageOpaqueInternal)PageGetSpecialPointer(lpage);
    rbuf = XLogReadBufferExtended(action->node, MAIN_FORKNUM, action->rightblk, RBM_NORMAL, &action->rightpblk);
    /* failure is impossible because we wrote this page earlier */
    if (BufferIsValid(rbuf))
        LockBuffer(rbuf, BUFFER_LOCK_EXCLUSIVE);
    else
        ereport(PANIC, (errmsg("btree_xlog_cleanup: right block unfound")));
    rpage = (Page)BufferGetPage(rbuf);
    rpageop = (BTPageOpaqueInternal)PageGetSpecialPointer(rpage);

    /* if the pages are all of their level, it's a only-page split */
    is_only = P_LEFTMOST(lpageop) && P_RIGHTMOST(rpageop);

    reln = CreateFakeRelcacheEntry(action->node);
    _bt_insert_parent(reln, lbuf, rbuf, NULL, action->is_root, is_only);
    FreeFakeRelcacheEntry(reln);
}

/* finish an incomplete deletion (of a half-dead page) */
void btree_xlog_finish_incomplete_deletion(const bt_incomplete_action *action)
{
    Buffer buf;
    buf = XLogReadBufferExtended(action->node, MAIN_FORKNUM, action->delblk, RBM_NORMAL, &action->delpblk);
    if (BufferIsValid(buf)) {
        Relation reln;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        reln = CreateFakeRelcacheEntry(action->node);
        if (_bt_pagedel(reln, buf, NULL) == 0) {
            ereport(PANIC, (errmsg("btree_xlog_cleanup: _bt_pagedel failed")));
        }
        FreeFakeRelcacheEntry(reln);
    }
}

void *btree_get_incomplete_actions()
{
    List *incompleteActions = t_thrd.xlog_cxt.incomplete_actions;
    t_thrd.xlog_cxt.incomplete_actions = NIL;
    return incompleteActions;
}

void btree_xlog_cleanup(void)
{
    ListCell *l = NULL;
    TimestampTz start_time = GetCurrentTimestamp();
    int64 duration = 0;
    MemoryContext oldCtx = NULL;
    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
    }

    if (log_min_messages <= DEBUG4) {
        ereport(LOG, (errmsg("[BTREE_ACTION_TRACE]btree_xlog_cleanup")));
    }

    /* we need to make sure we update root split and corresponding metapage first */
    foreach (l, t_thrd.xlog_cxt.incomplete_actions) {
        bt_incomplete_action *action = (bt_incomplete_action *)lfirst(l);
        t_thrd.xlog_cxt.imcompleteActionCnt++;
        ereport(WARNING, (errmsg("[BTREE_ACTION_TRACE]btree_xlog_cleanup: action spc:%u,db:%u,rel:%u,"
                                 "is_split:%d,is_root:%d,leftblk:%u,rightblk:%u,level:%u,delblk:%u,happen:%u,enable:%u",
                                 action->node.spcNode, action->node.dbNode, action->node.relNode, action->is_split,
                                 action->is_root, action->leftblk, action->rightblk, action->level, action->delblk,
                                 t_thrd.xlog_cxt.forceFinishHappened,
                                 g_instance.attr.attr_storage.enable_update_max_page_flush_lsn)));
        if (FORCE_FINISH_ENABLED) {
            continue;
        }
        if (action->is_split && action->is_root) {
            if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
                MemoryContext ctx = MemoryContextSwitchTo(oldCtx);
                btree_xlog_finish_incomplete_split(action);
                (void)MemoryContextSwitchTo(ctx);
            } else {
                btree_xlog_finish_incomplete_split(action);
            }
        }
    }

    foreach (l, t_thrd.xlog_cxt.incomplete_actions) {
        bt_incomplete_action *action = (bt_incomplete_action *)lfirst(l);
        ereport(WARNING, (errmsg("[BTREE_ACTION_TRACE]btree_xlog_cleanup2: action spc:%u,db:%u,rel:%u,"
                                 "is_split:%u,is_root:%u,leftblk:%u,rightblk:%u,level:%u,delblk:%u,happen:%u,enable:%u",
                                 action->node.spcNode, action->node.dbNode, action->node.relNode, action->is_split,
                                 action->is_root, action->leftblk, action->rightblk, action->level, action->delblk,
                                 t_thrd.xlog_cxt.forceFinishHappened,
                                 g_instance.attr.attr_storage.enable_update_max_page_flush_lsn)));
        if (FORCE_FINISH_ENABLED) {
            continue;
        }

        if (action->is_split) {
            if (!action->is_root) {
                if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
                    MemoryContext ctx = MemoryContextSwitchTo(oldCtx);
                    btree_xlog_finish_incomplete_split(action);
                    (void)MemoryContextSwitchTo(ctx);
                } else {
                    btree_xlog_finish_incomplete_split(action);
                }
            }
        } else {
            if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
                MemoryContext ctx = MemoryContextSwitchTo(oldCtx);
                btree_xlog_finish_incomplete_deletion(action);
                (void)MemoryContextSwitchTo(ctx);
            } else {
                btree_xlog_finish_incomplete_deletion(action);
            }
        }
    }
    t_thrd.xlog_cxt.incomplete_actions = NIL;

    if (get_real_recovery_parallelism() > 1 && (!parallel_recovery::DispatchPtrIsNull())) {
        (void)MemoryContextSwitchTo(oldCtx);
    }
    duration = GetCurrentTimestamp() - start_time;
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("btree_xlog_cleanup is over, it takes time:%ld microseconds", duration)));
}

bool btree_safe_restartpoint(void)
{
    if (t_thrd.xlog_cxt.incomplete_actions)
        return false;
    return true;
}

void btree_clear_imcompleteAction()
{
    if ((get_real_recovery_parallelism() > 1) && (!parallel_recovery::DispatchPtrIsNull())) {
        SwitchToDispatcherContext();
        t_thrd.xlog_cxt.incomplete_actions =
            parallel_recovery::CheckImcompleteAction(t_thrd.xlog_cxt.incomplete_actions);
        EndDispatcherContext();
    }
    btree_xlog_cleanup();
}
