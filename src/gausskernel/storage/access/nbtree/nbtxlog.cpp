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
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "access/multi_redo_api.h"

#ifdef ENABLE_UT
#define static
#endif

enum {
    BTREE_INSERT_ORIG_BLOCK_NUM = 0,
    BTREE_INSERT_CHILD_BLOCK_NUM,
    BTREE_INSERT_META_BLOCK_NUM,
};

enum {
    BTREE_SPLIT_LEFT_BLOCK_NUM = 0,
    BTREE_SPLIT_RIGHT_BLOCK_NUM,
    BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM,
    BTREE_SPLIT_CHILD_BLOCK_NUM
};

enum {
    BTREE_VACUUM_ORIG_BLOCK_NUM = 0,
};

enum {
    BTREE_DELETE_ORIG_BLOCK_NUM = 0,
};

enum {
    BTREE_DELETE_PAGE_TARGET_BLOCK_NUM = 0,
    BTREE_DELETE_PAGE_LEFT_BLOCK_NUM,
    BTREE_DELETE_PAGE_RIGHT_BLOCK_NUM,
    BTREE_DELETE_PAGE_PARENT_BLOCK_NUM,
    BTREE_DELETE_PAGE_META_BLOCK_NUM,
};

enum {
    BTREE_NEWROOT_ORIG_BLOCK_NUM = 0,
    BTREE_NEWROOT_LEFT_BLOCK_NUM,
    BTREE_NEWROOT_META_BLOCK_NUM
};

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
    uint32 level;
    /* these fields are for a delete: */
    BlockNumber delblk; /* parent block to be deleted */
} bt_incomplete_action;

static void log_incomplete_deletion(const RelFileNode* node, BlockNumber delblk)
{
    MemoryContext oldCtx = NULL;
    if (IsMultiThreadRedoRunning()) {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
    }
    bt_incomplete_action* action = (bt_incomplete_action*)palloc(sizeof(bt_incomplete_action));
    if (log_min_messages <= DEBUG4) {
        ereport(LOG,
            (errmsg("[BTREE_ACTION_TRACE]log_incomplete_deletion: spc:%u,db:%u,rel:%u,"
                    "delblk:%u",
                node->spcNode,
                node->dbNode,
                node->relNode,
                delblk)));
    }
    action->node = *node;
    action->is_split = false;
    action->delblk = delblk;
    t_thrd.xlog_cxt.incomplete_actions = lappend(t_thrd.xlog_cxt.incomplete_actions, action);
    if (IsMultiThreadRedoRunning()) {
        (void)MemoryContextSwitchTo(oldCtx);
    }
}

static void forget_matching_deletion(const RelFileNode* node, BlockNumber delblk)
{
    ListCell* l = NULL;

    MemoryContext oldCtx = NULL;
    if (IsMultiThreadRedoRunning()) {
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
        bt_incomplete_action* action = (bt_incomplete_action*)lfirst(l);

        if (RelFileNodeEquals(*node, action->node) && !action->is_split && delblk == action->delblk) {
            if (SHOW_DEBUG_MESSAGE()) {
                ereport(LOG,
                    (errmsg("[BTREE_ACTION_TRACE]forget_matching_deletion successfully: input spc:%u,db:%u,rel:%u,"
                            "delblk:%u, action spc:%u,db:%u,rel:%u,"
                            "is_split:%d,is_root:%d,leftblk:%u,rightblk:%u,level:%u,delblk:%u",
                        node->spcNode,
                        node->dbNode,
                        node->relNode,
                        delblk,
                        action->node.spcNode,
                        action->node.dbNode,
                        action->node.relNode,
                        action->is_split,
                        action->is_root,
                        action->leftblk,
                        action->rightblk,
                        action->level,
                        action->delblk)));
            }

            t_thrd.xlog_cxt.incomplete_actions = list_delete_ptr(t_thrd.xlog_cxt.incomplete_actions, action);
            pfree(action);
            break; /* need not look further */
        }
    }
    if (IsMultiThreadRedoRunning()) {
        (void)MemoryContextSwitchTo(oldCtx);
    }
}

static void _bt_restore_meta(XLogReaderState* record, uint8 block_id)
{
    RedoBufferInfo metabuf;
    char* ptr = NULL;
    Size len;

    XLogInitBufferForRedo(record, block_id, &metabuf);
    ptr = XLogRecGetBlockData(record, block_id, &len);
    btree_restore_meta_operator_page(&metabuf, (void*)ptr, len);

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
        btree_xlog_clear_incomplete_split(&buffer);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void btree_xlog_insert(bool isleaf, bool ismeta, XLogReaderState* record)
{
    xl_btree_insert* xlrec = (xl_btree_insert*)XLogRecGetData(record);
    RedoBufferInfo buffer;
    char* datapos = NULL;

    /*
     * Insertion to an internal page finishes an incomplete split at the child
     * level.  Clear the incomplete-split flag in the child.  Note: during
     * normal operation, the child and parent pages are locked at the same
     * time, so that clearing the flag and inserting the downlink appear
     * atomic to other backends.  We don't bother with that during replay,
     * because readers don't care about the incomplete-split flag and there
     * cannot be updates happening.
     */
    if (!isleaf) {
        _bt_clear_incomplete_split(record, BTREE_INSERT_CHILD_BLOCK_NUM);
    }

    if (XLogReadBufferForRedo(record, BTREE_INSERT_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        Size datalen;

        datapos = XLogRecGetBlockData(record, BTREE_INSERT_ORIG_BLOCK_NUM, &datalen);
        btree_xlog_insert_operator_page(&buffer, (void*)xlrec, (void*)datapos, datalen);
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
    if (ismeta) {
        _bt_restore_meta(record, BTREE_INSERT_META_BLOCK_NUM);
    }
}

static void btree_xlog_split(bool onleft, bool isroot, XLogReaderState* record)
{
    xl_btree_split* xlrec = (xl_btree_split*)XLogRecGetData(record);
    bool isleaf = (xlrec->level == 0);
    RedoBufferInfo lbuf;
    RedoBufferInfo rbuf;
    char* datapos = NULL;
    Size datalen;
    RelFileNode rnode;
    BlockNumber leftsib;
    BlockNumber rightsib;
    BlockNumber rnext;
    Item left_hikey = NULL;
    Size left_hikeysz = 0;

    XLogRecGetBlockTag(record, BTREE_SPLIT_LEFT_BLOCK_NUM, &rnode, NULL, &leftsib);
    XLogRecGetBlockTag(record, BTREE_SPLIT_RIGHT_BLOCK_NUM, NULL, NULL, &rightsib);
    if (!XLogRecGetBlockTag(record, BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM, NULL, NULL, &rnext)) {
        rnext = P_NONE;
    }

    /* Forget any split this insertion completes */
    if (!isleaf) {
        _bt_clear_incomplete_split(record, BTREE_SPLIT_CHILD_BLOCK_NUM);
    }

    /* Reconstruct right (new) sibling page from scratch */
    XLogInitBufferForRedo(record, BTREE_SPLIT_RIGHT_BLOCK_NUM, &rbuf);
    datapos = XLogRecGetBlockData(record, BTREE_SPLIT_RIGHT_BLOCK_NUM, &datalen);
    btree_xlog_split_operator_rightpage(&rbuf, (void*)xlrec, leftsib, rnext, (void*)datapos, datalen);
    MarkBufferDirty(rbuf.buf);

    /* don't release the buffer yet; we touch right page's first item below
     * Now reconstruct left (original) sibling page 
     */
    if (XLogReadBufferForRedo(record, BTREE_SPLIT_LEFT_BLOCK_NUM, &lbuf) == BLK_NEEDS_REDO) {
        datapos = XLogRecGetBlockData(record, BTREE_SPLIT_LEFT_BLOCK_NUM, &datalen);
        btree_xlog_split_operator_leftpage(&lbuf, (void*)xlrec, rightsib, onleft, (void*)datapos, datalen, left_hikey, left_hikeysz);
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

        if (XLogReadBufferForRedo(record, BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
            btree_xlog_split_operator_nextpage(&buffer, rightsib);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }
}

static void btree_xlog_vacuum(XLogReaderState* record)
{
    xl_btree_vacuum* xlrec = (xl_btree_vacuum*)XLogRecGetData(record);
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

        for (blkno = xlrec->lastBlockVacuumed + 1; blkno < thisblkno; blkno++) {
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
            Buffer buffer = XLogReadBufferExtended(thisrnode, MAIN_FORKNUM, blkno, RBM_NORMAL_NO_LOG);
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
        char* ptr = NULL;
        Size len;

        ptr = XLogRecGetBlockData(record, BTREE_VACUUM_ORIG_BLOCK_NUM, &len);
        btree_xlog_vacuum_operator_page(&redobuf, (void*)xlrec, (void*)ptr, len);
        MarkBufferDirty(redobuf.buf);
    }
    if (BufferIsValid(redobuf.buf))
        UnlockReleaseBuffer(redobuf.buf);
}

static void btree_xlog_delete(XLogReaderState* record)
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
        btree_xlog_delete_operator_page(&buffer, (void*)XLogRecGetData(record), XLogRecGetDataLen(record));

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void btree_xlog_delete_page(uint8 info, XLogReaderState* record)
{
    xl_btree_delete_page* xlrec = (xl_btree_delete_page*)XLogRecGetData(record);
    RelFileNode rnode;
    BlockNumber parent;
    BlockNumber target;
    RedoBufferInfo buffer;

    XLogRecGetBlockTag(record, BTREE_DELETE_PAGE_TARGET_BLOCK_NUM, &rnode, NULL, &target);
    XLogRecGetBlockTag(record, BTREE_DELETE_PAGE_PARENT_BLOCK_NUM, NULL, NULL, &parent);

    /*
     * In normal operation, we would lock all the pages this WAL record
     * touches before changing any of them.  In WAL replay, it should be okay
     * to lock just one page at a time, since no concurrent index updates can
     * be happening, and readers should not care whether they arrive at the
     * target page or not (since it's surely empty).
     * 
     * parent page
     */
    if (XLogReadBufferForRedo(record, BTREE_DELETE_PAGE_PARENT_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        btree_xlog_delete_page_operator_parentpage(&buffer, (void*)xlrec, info);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }

    /* Fix left-link of right sibling */
    if (XLogReadBufferForRedo(record, BTREE_DELETE_PAGE_RIGHT_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        btree_xlog_delete_page_operator_rightpage(&buffer, (void*)xlrec);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }

    /* Fix right-link of left sibling, if any */
    if (xlrec->leftblk != P_NONE) {
        if (XLogReadBufferForRedo(record, BTREE_DELETE_PAGE_LEFT_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
            btree_xlog_delete_page_operator_leftpage(&buffer, (void*)xlrec);
            MarkBufferDirty(buffer.buf);
        }
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }

    /* Rewrite target page as empty deleted page */
    XLogInitBufferForRedo(record, BTREE_DELETE_PAGE_TARGET_BLOCK_NUM, &buffer);
    btree_xlog_delete_page_operator_currentpage(&buffer, (void*)xlrec);

    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    /* Update metapage if needed */
    if (info == XLOG_BTREE_DELETE_PAGE_META) {
        _bt_restore_meta(record, BTREE_DELETE_PAGE_META_BLOCK_NUM);
    }

    /* Forget any completed deletion */
    forget_matching_deletion(&rnode, target);

    /* If parent became half-dead, remember it for deletion */
    if (info == XLOG_BTREE_DELETE_PAGE_HALF) {
        log_incomplete_deletion(&rnode, parent);
    }
}

static void btree_xlog_newroot(XLogReaderState* record)
{
    xl_btree_newroot* xlrec = (xl_btree_newroot*)XLogRecGetData(record);
    BlockNumber downlink = 0;
    RedoBufferInfo buffer;
    RedoBufferInfo lbuffer;
    char* ptr = NULL;
    Size len;

    XLogInitBufferForRedo(record, BTREE_NEWROOT_ORIG_BLOCK_NUM, &buffer);
    ptr = XLogRecGetBlockData(record, BTREE_NEWROOT_ORIG_BLOCK_NUM, &len);
    btree_xlog_newroot_operator_page(&buffer, (void*)xlrec, (void*)ptr, len, &downlink);

    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    lbuffer.buf = InvalidBuffer;
    if (xlrec->level > 0 && XLogReadBufferForRedo(record, BTREE_NEWROOT_LEFT_BLOCK_NUM, &lbuffer) == BLK_NEEDS_REDO) {
        btree_xlog_clear_incomplete_split(&lbuffer);
        MarkBufferDirty(lbuffer.buf);
    }
    
    if (BufferIsValid(lbuffer.buf)) {
        UnlockReleaseBuffer(lbuffer.buf);
    }
    _bt_restore_meta(record, BTREE_NEWROOT_META_BLOCK_NUM);
}

bool IsBtreeVacuum(const XLogReaderState* record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (XLogRecGetRmid(record) == RM_BTREE_ID) {
        if ((info == XLOG_BTREE_REUSE_PAGE) || (info == XLOG_BTREE_VACUUM) || (info == XLOG_BTREE_DELETE) ||
            (info == XLOG_BTREE_DELETE_PAGE) || (info == XLOG_BTREE_DELETE_PAGE_META) ||
            (info == XLOG_BTREE_DELETE_PAGE_HALF)) {
            return true;
        }
    }

    return false;
}

static void btree_xlog_reuse_page(XLogReaderState* record)
{
    xl_btree_reuse_page* xlrec = (xl_btree_reuse_page*)XLogRecGetData(record);

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

    if (InHotStandby && SUPPORT_HOT_STANDBY) {
        ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, tmp_node);
    }
}

void btree_redo(XLogReaderState* record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_BTREE_INSERT_LEAF:
            btree_xlog_insert(true, false, record);
            break;
        case XLOG_BTREE_INSERT_UPPER:
            btree_xlog_insert(false, false, record);
            break;
        case XLOG_BTREE_INSERT_META:
            btree_xlog_insert(false, true, record);
            break;
        case XLOG_BTREE_SPLIT_L:
            btree_xlog_split(true, false, record);
            break;
        case XLOG_BTREE_SPLIT_R:
            btree_xlog_split(false, false, record);
            break;
        case XLOG_BTREE_SPLIT_L_ROOT:
            btree_xlog_split(true, true, record);
            break;
        case XLOG_BTREE_SPLIT_R_ROOT:
            btree_xlog_split(false, true, record);
            break;
        case XLOG_BTREE_VACUUM:
            btree_xlog_vacuum(record);
            break;
        case XLOG_BTREE_DELETE:
            btree_xlog_delete(record);
            break;
        case XLOG_BTREE_DELETE_PAGE:
        case XLOG_BTREE_DELETE_PAGE_META:
        case XLOG_BTREE_DELETE_PAGE_HALF:
            btree_xlog_delete_page(info, record);
            break;
        case XLOG_BTREE_NEWROOT:
            btree_xlog_newroot(record);
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

/* finish an incomplete deletion (of a half-dead page) */
void btree_xlog_finish_incomplete_deletion(const bt_incomplete_action* action)
{
    Buffer buf;
    buf = XLogReadBufferExtended(action->node, MAIN_FORKNUM, action->delblk, RBM_NORMAL);
    if (BufferIsValid(buf)) {
        Relation reln;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        reln = CreateFakeRelcacheEntry(action->node);
        if (_bt_pagedel(reln, buf, NULL) == 0)
            ereport(PANIC, (errmsg("btree_xlog_cleanup: _bt_pagedel failed")));
        FreeFakeRelcacheEntry(reln);
    }
}

void* BTreeGetIncompleteActions()
{
    List* incompleteActions = t_thrd.xlog_cxt.incomplete_actions;
    t_thrd.xlog_cxt.incomplete_actions = NIL;
    return incompleteActions;
}

