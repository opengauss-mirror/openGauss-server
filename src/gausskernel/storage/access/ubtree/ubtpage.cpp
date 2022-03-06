/* -------------------------------------------------------------------------
 *
 * ubtpage.cpp
 *	  BTree-specific page management code for the openGauss btree access
 *	  method.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/ubtree/ubtpage.cpp
 *
 *	NOTES
 *	   openGauss btree pages look like ordinary relation pages.	The opaque
 *	   data at high addresses includes pointers to left and right siblings
 *	   and flag data describing page state.  The first page in a btree, page
 *	   zero, is special -- it stores meta-information describing the tree.
 *	   Pages one and higher store the actual tree data.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hio.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/freespace.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "utils/inval.h"
#include "utils/snapmgr.h"

static bool UBTreeMarkPageHalfDead(Relation rel, Buffer leafbuf, BTStack stack);
static bool UBTreeUnlinkHalfDeadPage(Relation rel, Buffer leafbuf, bool *rightsib_empty);
static bool UBTreeLockBranchParent(Relation rel, BlockNumber child, BTStack stack, Buffer *topparent,
    OffsetNumber *topoff, BlockNumber *target, BlockNumber *rightsib);
static void UBTreeLogReusePage(Relation rel, BlockNumber blkno, TransactionId latestRemovedXid);

/*
 *	UBTreePageInit() -- Initialize a new page.
 *
 * On return, the page header is initialized; data space is empty;
 * special space is zeroed out.
 */
void UBTreePageInit(Page page, Size size)
{
    PageInit(page, size, sizeof(UBTPageOpaqueData));
    ((UBTPageOpaque)PageGetSpecialPointer(page))->xact = 0;
}

/*
 *	UBTreeInitMetaPage() -- Fill a page buffer with a correct metapage image
 */
void UBTreeInitMetaPage(Page page, BlockNumber rootbknum, uint32 level)
{
    BTMetaPageData *metad = NULL;
    UBTPageOpaqueInternal metaopaque;

    UBTreePageInit(page, BLCKSZ);

    metad = BTPageGetMeta(page);
    metad->btm_magic = BTREE_MAGIC;
    metad->btm_version = BTREE_VERSION;
    metad->btm_root = rootbknum;
    metad->btm_level = level;
    metad->btm_fastroot = rootbknum;
    metad->btm_fastlevel = level;

    metaopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    metaopaque->btpo_flags = BTP_META;

    /*
     * Set pd_lower just past the end of the metadata.	This is not essential
     * but it makes the page look compressible to xlog.c.
     */
    ((PageHeader)page)->pd_lower = (uint16)(((char *)metad + sizeof(BTMetaPageData)) - (char *)page);
}

/*
 *	UBTreeGetRoot() -- Get the root page of the btree.
 *
 *		Since the root page can move around the btree file, we have to read
 *		its location from the metadata page, and then read the root page
 *		itself.  If no root page exists yet, we have to create one.  The
 *		standard class of race conditions exists here; I think I covered
 *		them all in the Hopi Indian rain dance of lock requests below.
 *
 *		The access type parameter (BT_READ or BT_WRITE) controls whether
 *		a new root page will be created or not.  If access = BT_READ,
 *		and no root page exists, we just return InvalidBuffer.	For
 *		BT_WRITE, we try to create the root page if it doesn't exist.
 *		NOTE that the returned root page will have only a read lock set
 *		on it even if access = BT_WRITE!
 *
 *		The returned page is not necessarily the true root --- it could be
 *		a "fast root" (a page that is alone in its level due to deletions).
 *		Also, if the root page is split while we are "in flight" to it,
 *		what we will return is the old root, which is now just the leftmost
 *		page on a probably-not-very-wide level.  For most purposes this is
 *		as good as or better than the true root, so we do not bother to
 *		insist on finding the true root.  We do, however, guarantee to
 *		return a live (not deleted or half-dead) page.
 *
 *		On successful return, the root page is pinned and read-locked.
 *		The metadata page is not locked or pinned on exit.
 */
Buffer UBTreeGetRoot(Relation rel, int access)
{
    Buffer metabuf;
    Page metapg;
    UBTPageOpaqueInternal metaopaque;
    Buffer rootbuf;
    Page rootpage;
    UBTPageOpaqueInternal rootopaque;
    BlockNumber rootblkno;
    uint32 rootlevel;
    BTMetaPageData *metad = NULL;

    /*
     * Try to use previously-cached metapage data to find the root.  This
     * normally saves one buffer access per index search, which is a very
     * helpful savings in bufmgr traffic and hence contention.
     */
    if (rel->rd_amcache != NULL) {
        bool isRootCacheValid = false;
        metad = (BTMetaPageData*)rel->rd_amcache;
        /* We shouldn't have cached it if any of these fail */
        Assert(metad->btm_magic == BTREE_MAGIC);
        Assert(metad->btm_version == BTREE_VERSION);
        Assert(metad->btm_root != P_NONE);

        rootblkno = metad->btm_fastroot;
        Assert(rootblkno != P_NONE);
        rootlevel = metad->btm_fastlevel;

        /*
         * Great than 0 means shared buffer. Global temp table does not suppport this optimization.
         * Be careful: temp tables use shared buffer, please refer to RelationBuildLocalRelation to
         * check the value of rel->rd_backend
         */
        if (likely(rel->rd_rootcache > 0)) {
            bool valid = false;
            // use cached value, but with double checks to make sure
            rootbuf = rel->rd_rootcache;
            Assert(rootblkno != P_NEW);

            // below is copied from ReadBuffer
            RelationOpenSmgr(rel);

            /* Make sure we will have room to remember the buffer pin */
            ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

            BufferDesc *buf = GetBufferDescriptor(rootbuf - 1); // caveat for GetBufferDescriptor for -1!
            valid = PinBuffer(buf, NULL);
            if (valid) {
                LockBuffer(rootbuf, BT_READ);
                isRootCacheValid = RelFileNodeEquals(buf->tag.rnode, rel->rd_node) && (buf->tag.blockNum == rootblkno);
                if (!isRootCacheValid)
                    UnlockReleaseBuffer(rootbuf);
            } else {
                UnpinBuffer(buf, true);
            }
        }
        if (!isRootCacheValid) {
            rootbuf = _bt_getbuf(rel, rootblkno, BT_READ);
            // cache it
            rel->rd_rootcache = rootbuf;

            /* BM_IS_ROOT is not used nor unset. Dont set it for now. */
            // prevent it from page out
        }

        rootpage = BufferGetPage(rootbuf);
        rootopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(rootpage);
        /*
         * Since the cache might be stale, we check the page more carefully
         * here than normal.  We *must* check that it's not deleted. If it's
         * not alone on its level, then we reject too --- this may be overly
         * paranoid but better safe than sorry.  Note we don't check P_ISROOT,
         * because that's not set in a "fast root".
         */
        if (!P_IGNORE(rootopaque) && rootopaque->btpo.level == rootlevel && P_LEFTMOST(rootopaque) &&
            P_RIGHTMOST(rootopaque)) {
            /* OK, accept cached page as the root */
            return rootbuf;
        }
        _bt_relbuf(rel, rootbuf);
        /* Cache is stale, throw it away */
        if (rel->rd_amcache)
            pfree(rel->rd_amcache);
        rel->rd_amcache = NULL;
        rel->rd_rootcache = InvalidBuffer;
    }

    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_READ);
    metapg = BufferGetPage(metabuf);
    metaopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(metapg);
    metad = BTPageGetMeta(metapg);
    /* sanity-check the metapage */
    if (!(metaopaque->btpo_flags & BTP_META) || metad->btm_magic != BTREE_MAGIC)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("index \"%s\" is not a btree", RelationGetRelationName(rel))));

    if (metad->btm_version != BTREE_VERSION)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("version mismatch in index \"%s\": file version %u, code version %d",
                       RelationGetRelationName(rel), metad->btm_version, BTREE_VERSION)));

    /* if no root page initialized yet, do it */
    if (metad->btm_root == P_NONE) {
        /* If access = BT_READ, caller doesn't want us to create root yet */
        if (access == BT_READ) {
            _bt_relbuf(rel, metabuf);
            return InvalidBuffer;
        }

        /* trade in our read lock for a write lock */
        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(metabuf, BT_WRITE);

        /*
         * Race condition:	if someone else initialized the metadata between
         * the time we released the read lock and acquired the write lock, we
         * must avoid doing it again.
         */
        if (metad->btm_root != P_NONE) {
            /*
             * Metadata initialized by someone else.  In order to guarantee no
             * deadlocks, we have to release the metadata page and start all
             * over again.	(Is that really true? But it's hardly worth trying
             * to optimize this case.)
             */
            _bt_relbuf(rel, metabuf);
            return UBTreeGetRoot(rel, access);
        }

        /*
         * Get, initialize, write, and leave a lock of the appropriate type on
         * the new root page.  Since this is the first page in the tree, it's
         * a leaf as well as the root.
         *      NOTE: after the page is absolutely used, call UBTreeRecordUsedPage()
         *            before we release the Exclusive lock.
         */
        UBTRecycleQueueAddress addr;
        rootbuf = UBTreeGetNewPage(rel, &addr);
        rootblkno = BufferGetBlockNumber(rootbuf);
        rootpage = BufferGetPage(rootbuf);
        rootopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(rootpage);
        rootopaque->btpo_prev = rootopaque->btpo_next = P_NONE;
        rootopaque->btpo_flags = (BTP_LEAF | BTP_ROOT);
        rootopaque->btpo.level = 0;
        rootopaque->btpo_cycleid = 0;

        /* NO ELOG(ERROR) till meta is updated */
        START_CRIT_SECTION();

        metad->btm_root = rootblkno;
        metad->btm_level = 0;
        metad->btm_fastroot = rootblkno;
        metad->btm_fastlevel = 0;

        MarkBufferDirty(rootbuf);
        MarkBufferDirty(metabuf);

        /* XLOG stuff */
        if (RelationNeedsWAL(rel)) {
            xl_btree_newroot xlrec;
            xl_btree_metadata md;
            XLogRecPtr recptr;

            XLogBeginInsert();
            XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
            XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT);

            md.root = rootblkno;
            md.level = 0;
            md.fastroot = rootblkno;
            md.fastlevel = 0;

            XLogRegisterBufData(2, (char *)&md, sizeof(xl_btree_metadata));

            xlrec.rootblk = rootblkno;
            xlrec.level = 0;
            XLogRegisterData((char *)&xlrec, SizeOfBtreeNewroot);

            recptr = XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_NEWROOT);

            PageSetLSN(rootpage, recptr);
            PageSetLSN(metapg, recptr);
        }

        END_CRIT_SECTION();

        /* discard this page from the Recycle Queue */
        UBTreeRecordUsedPage(rel, addr);

        /*
         * swap root write lock for read lock.	There is no danger of anyone
         * else accessing the new root page while it's unlocked, since no one
         * else knows where it is yet.
         */
        LockBuffer(rootbuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(rootbuf, BT_READ);

        /* okay, metadata is correct, release lock on it */
        _bt_relbuf(rel, metabuf);
        rel->rd_rootcache = InvalidBuffer;
    } else {
        rootblkno = metad->btm_fastroot;
        Assert(rootblkno != P_NONE);
        rootlevel = metad->btm_fastlevel;

        /*
         * Cache the metapage data for next time
         */
        rel->rd_amcache = MemoryContextAlloc(rel->rd_indexcxt, sizeof(BTMetaPageData));
        errno_t rc = memcpy_s(rel->rd_amcache, sizeof(BTMetaPageData), metad, sizeof(BTMetaPageData));
        securec_check(rc, "", "");

        rel->rd_rootcache = InvalidBuffer;
        /*
         * We are done with the metapage; arrange to release it via first
         * _bt_relandgetbuf call
         */
        rootbuf = metabuf;

        for (;;) {
            rootbuf = _bt_relandgetbuf(rel, rootbuf, rootblkno, BT_READ);
            rootpage = BufferGetPage(rootbuf);
            rootopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(rootpage);
            if (!P_IGNORE(rootopaque))
                break;

            /* it's dead, Jim.  step right one page */
            if (P_RIGHTMOST(rootopaque))
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("no live root page found in index \"%s\"", RelationGetRelationName(rel))));
            rootblkno = rootopaque->btpo_next;
        }

        /* Note: can't check btpo.level on deleted pages */
        if (rootopaque->btpo.level != rootlevel)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("root page %u of index \"%s\" has level %u, expected %u", rootblkno,
                           RelationGetRelationName(rel), rootopaque->btpo.level, rootlevel)));
    }

    /*
     * By here, we have a pin and read lock on the root page, and no lock set
     * on the metadata page.  Return the root page's buffer.
     */
    return rootbuf;
}

bool UBTreePageRecyclable(Page page)
{
    /*
     * It's possible to find an all-zeroes page in an index --- for example, a
     * backend might successfully extend the relation one page and then crash
     * before it is able to make a WAL entry for adding the page. If we find a
     * zeroed page then reclaim it.
     */
    TransactionId frozenXmin = g_instance.undo_cxt.oldestFrozenXid;
    if (PageIsNew(page)) {
        return true;
    }

    /*
     * Otherwise, recycle if deleted and too old to have any processes
     * interested in it.
     */
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    return P_ISDELETED(opaque) && TransactionIdPrecedes(((UBTPageOpaque)opaque)->xact, frozenXmin);
}

/*
 * UBTreeIsPageHalfDead() -- Returns true, if the given block has the half-dead flag set.
 */
static bool UBTreeIsPageHalfDead(Relation rel, BlockNumber blk)
{
    Buffer buf;
    Page page;
    UBTPageOpaqueInternal opaque;
    bool result;

    buf = _bt_getbuf(rel, blk, BT_READ);
    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    result = P_ISHALFDEAD(opaque);
    _bt_relbuf(rel, buf);

    return result;
}

/*
 * Subroutine to find the parent of the branch we're deleting.  This climbs
 * up the tree until it finds a page with more than one child, i.e. a page
 * that will not be totally emptied by the deletion.  The chain of pages below
 * it, with one downlink each, will form the branch that we need to delete.
 *
 * If we cannot remove the downlink from the parent, because it's the
 * rightmost entry, returns false.  On success, *topparent and *topoff are set
 * to the buffer holding the parent, and the offset of the downlink in it.
 * *topparent is write-locked, the caller is responsible for releasing it when
 * done.  *target is set to the topmost page in the branch to-be-deleted, i.e.
 * the page whose downlink *topparent / *topoff point to, and *rightsib to its
 * right sibling.
 *
 * "child" is the leaf page we wish to delete, and "stack" is a search stack
 * leading to it (it actually leads to the leftmost leaf page with a high key
 * matching that of the page to be deleted in !heapkeyspace indexes).  Note
 * that we will update the stack entry(s) to reflect current downlink
 * positions --- this is essentially the same as the corresponding step of
 * splitting, and is not expected to affect caller.  The caller should
 * initialize *target and *rightsib to the leaf page and its right sibling.
 *
 * Note: it's OK to release page locks on any internal pages between the leaf
 * and *topparent, because a safe deletion can't become unsafe due to
 * concurrent activity.  An internal page can only acquire an entry if the
 * child is split, but that cannot happen as long as we hold a lock on the
 * leaf.
 */
static bool UBTreeLockBranchParent(Relation rel, BlockNumber child, BTStack stack, Buffer *topparent,
    OffsetNumber *topoff, BlockNumber *target, BlockNumber *rightsib)
{
    BlockNumber parent;
    OffsetNumber poffset;
    OffsetNumber maxoff;
    Buffer pbuf;
    Page page;
    UBTPageOpaqueInternal opaque;
    BlockNumber leftsib;

    /*
     * Locate the downlink of "child" in the parent, updating the stack entry
     * if needed.  This is how !heapkeyspace indexes deal with having
     * non-unique high keys in leaf level pages.  Even heapkeyspace indexes
     * can have a stale stack due to insertions into the parent.
     */
    stack->bts_btentry = *target;
    pbuf = UBTreeGetStackBuf(rel, stack);
    if (pbuf == InvalidBuffer) {
        elog(ERROR, "failed to re-find parent key in index \"%s\" for deletion target page %u",
             RelationGetRelationName(rel), child);
    }
    parent = stack->bts_blkno;
    poffset = stack->bts_offset;

    page = BufferGetPage(pbuf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    maxoff = PageGetMaxOffsetNumber(page);
    /*
     * If the target is the rightmost child of its parent, then we can't
     * delete, unless it's also the only child.
     */
    if (poffset >= maxoff) {
        /* It's rightmost child... */
        if (poffset == P_FIRSTDATAKEY(opaque)) {
            /*
             * It's only child, so safe if parent would itself be removable.
             * We have to check the parent itself, and then recurse to test
             * the conditions at the parent's parent.
             */
            if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_INCOMPLETE_SPLIT(opaque)) {
                _bt_relbuf(rel, pbuf);
                return false;
            }

            *target = parent;
            *rightsib = opaque->btpo_next;
            leftsib = opaque->btpo_prev;

            _bt_relbuf(rel, pbuf);

            /*
             * Like in _bt_pagedel, check that the left sibling is not marked
             * with INCOMPLETE_SPLIT flag.  That would mean that there is no
             * downlink to the page to be deleted, and the page deletion
             * algorithm isn't prepared to handle that.
             */
            if (leftsib != P_NONE) {
                Buffer lbuf;
                Page lpage;
                UBTPageOpaqueInternal lopaque;

                lbuf = _bt_getbuf(rel, leftsib, BT_READ);
                lpage = BufferGetPage(lbuf);
                lopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(lpage);
                /*
                 * If the left sibling was concurrently split, so that its
                 * next-pointer doesn't point to the current page anymore, the
                 * split that created the current page must be completed. (We
                 * don't allow splitting an incompletely split page again
                 * until the previous split has been completed)
                 */
                if (lopaque->btpo_next == parent && P_INCOMPLETE_SPLIT(lopaque)) {
                    _bt_relbuf(rel, lbuf);
                    return false;
                }
                _bt_relbuf(rel, lbuf);
            }

            return UBTreeLockBranchParent(rel, parent, stack->bts_parent, topparent, topoff, target, rightsib);
        } else {
            /* Unsafe to delete */
            _bt_relbuf(rel, pbuf);
            return false;
        }
    } else {
        /* Not rightmost child, so safe to delete */
        *topparent = pbuf;
        *topoff = poffset;
        return true;
    }
}

/*
 * UBTreePageDel() -- Delete a page from the b-tree, if legal to do so.
 *
 * This action unlinks the page from the b-tree structure, removing all
 * pointers leading to it --- but not touching its own left and right links.
 * The page cannot be physically reclaimed right away, since other processes
 * may currently be trying to follow links leading to the page; they have to
 * be allowed to use its right-link to recover.  See nbtree/README.
 *
 * On entry, the target buffer must be pinned and locked (either read or write
 * lock is OK).  This lock and pin will be dropped before exiting.
 *
 * Returns the number of pages successfully deleted (zero if page cannot
 * be deleted now; could be more than one if parent or sibling pages were
 * deleted too).
 *
 * NOTE: this leaks memory.  Rather than trying to clean up everything
 * carefully, it's better to run it in a temp context that can be reset
 * frequently.
 */
int UBTreePageDel(Relation rel, Buffer buf)
{
    int ndeleted = 0;
    BlockNumber rightsib;
    bool rightsib_empty = false;
    Page page;
    UBTPageOpaqueInternal opaque;

    WHITEBOX_TEST_STUB("UBTreePageDel", WhiteboxDefaultErrorEmit);

    /*
     * "stack" is a search stack leading (approximately) to the target page.
     * It is initially NULL, but when iterating, we keep it to avoid
     * duplicated search effort.
     *
     * Also, when "stack" is not NULL, we have already checked that the
     * current page is not the right half of an incomplete split, i.e. the
     * left sibling does not have its INCOMPLETE_SPLIT flag set.
     */
    BTStack stack = NULL;

    for (;;) {
        page = BufferGetPage(buf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        /*
         * Internal pages are never deleted directly, only as part of deleting
         * the whole branch all the way down to leaf level.
         */
        if (!P_ISLEAF(opaque)) {
            /*
             * Pre-9.4 page deletion only marked internal pages as half-dead,
             * but now we only use that flag on leaf pages. The old algorithm
             * was never supposed to leave half-dead pages in the tree, it was
             * just a transient state, but it was nevertheless possible in
             * error scenarios. We don't know how to deal with them here. They
             * are harmless as far as searches are considered, but inserts
             * into the deleted keyspace could add out-of-order downlinks in
             * the upper levels. Log a notice, hopefully the admin will notice
             * and reindex.
             */
            if (P_ISHALFDEAD(opaque)) {
                ereport(LOG, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("index \"%s\" contains a half-dead internal page", RelationGetRelationName(rel)),
                    errhint("This can be caused by an interrupted VACUUM in version 9.3 or older, before upgrade. "
                    "Please REINDEX it.")));
            }
            _bt_relbuf(rel, buf);
            return ndeleted;
        }

        /*
         * We can never delete rightmost pages nor root pages.  While at it,
         * check that page is not already deleted and is empty.
         *
         * To keep the algorithm simple, we also never delete an incompletely
         * split page (they should be rare enough that this doesn't make any
         * meaningful difference to disk usage):
         *
         * The INCOMPLETE_SPLIT flag on the page tells us if the page is the
         * left half of an incomplete split, but ensuring that it's not the
         * right half is more complicated.  For that, we have to check that
         * the left sibling doesn't have its INCOMPLETE_SPLIT flag set.  On
         * the first iteration, we temporarily release the lock on the current
         * page, and check the left sibling and also construct a search stack
         * to.  On subsequent iterations, we know we stepped right from a page
         * that passed these tests, so it's OK.
         */
        if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_ISDELETED(opaque) ||
            P_FIRSTDATAKEY(opaque) <= PageGetMaxOffsetNumber(page) || P_INCOMPLETE_SPLIT(opaque)) {
            /* Should never fail to delete a half-dead page */
            Assert(!P_ISHALFDEAD(opaque));

            _bt_relbuf(rel, buf);
            return ndeleted;
        }

        /*
         * First, remove downlink pointing to the page (or a parent of the
         * page, if we are going to delete a taller branch), and mark the page
         * as half-dead.
         */
        if (!P_ISHALFDEAD(opaque)) {
            /*
             * We need an approximate pointer to the page's parent page.  We
             * use a variant of the standard search mechanism to search for
             * the page's high key; this will give us a link to either the
             * current parent or someplace to its left (if there are multiple
             * equal high keys, which is possible with !heapkeyspace indexes).
             *
             * Also check if this is the right-half of an incomplete split
             * (see comment above).
             */
            if (!stack) {
                BTScanInsert itup_key;
                ItemId itemid;
                IndexTuple targetkey;
                Buffer lbuf;
                BlockNumber leftsib;

                itemid = PageGetItemId(page, P_HIKEY);
                targetkey = CopyIndexTuple((IndexTuple)PageGetItem(page, itemid));

                leftsib = opaque->btpo_prev;

                /*
                 * To avoid deadlocks, we'd better drop the leaf page lock
                 * before going further.
                 */
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);

                /*
                 * Fetch the left sibling, to check that it's not marked with
                 * INCOMPLETE_SPLIT flag.  That would mean that the page
                 * to-be-deleted doesn't have a downlink, and the page
                 * deletion algorithm isn't prepared to handle that.
                 */
                if (!P_LEFTMOST(opaque)) {
                    UBTPageOpaqueInternal lopaque;
                    Page lpage;

                    lbuf = _bt_getbuf(rel, leftsib, BT_READ);
                    lpage = BufferGetPage(lbuf);
                    lopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(lpage);
                    /*
                     * If the left sibling is split again by another backend,
                     * after we released the lock, we know that the first
                     * split must have finished, because we don't allow an
                     * incompletely-split page to be split again.  So we don't
                     * need to walk right here.
                     */
                    if (lopaque->btpo_next == BufferGetBlockNumber(buf) && P_INCOMPLETE_SPLIT(lopaque)) {
                        ReleaseBuffer(buf);
                        _bt_relbuf(rel, lbuf);
                        return ndeleted;
                    }
                    _bt_relbuf(rel, lbuf);
                }

                /* we need an insertion scan key for the search, so build one */
                itup_key = UBTreeMakeScanKey(rel, targetkey);
                /* find the leftmost leaf page with matching pivot/high key */
                itup_key->pivotsearch = true;
                stack = UBTreeSearch(rel, itup_key, &lbuf, BT_READ);
                /* don't need a lock or second pin on the page */
                _bt_relbuf(rel, lbuf);

                /*
                 * Re-lock the leaf page, and start over, to re-check that the
                 * page can still be deleted.
                 */
                LockBuffer(buf, BT_WRITE);
                continue;
            }

            if (!UBTreeMarkPageHalfDead(rel, buf, stack)) {
                _bt_relbuf(rel, buf);
                return ndeleted;
            }
        }

        /*
         * Then unlink it from its siblings.  Each call to
         * _bt_unlink_halfdead_page unlinks the topmost page from the branch,
         * making it shallower.  Iterate until the leaf page is gone.
         */
        rightsib_empty = false;
        while (P_ISHALFDEAD(opaque)) {
            /* will check for interrupts, once lock is released */
            if (!UBTreeUnlinkHalfDeadPage(rel, buf, &rightsib_empty)) {
                /* _bt_unlink_halfdead_page already released buffer */
                return ndeleted;
            }
            ndeleted++;
        }

        rightsib = opaque->btpo_next;

        _bt_relbuf(rel, buf);

        /*
         * Check here, as calling loops will have locks held, preventing
         * interrupts from being processed.
         */
        CHECK_FOR_INTERRUPTS();

        /*
         * The page has now been deleted. If its right sibling is completely
         * empty, it's possible that the reason we haven't deleted it earlier
         * is that it was the rightmost child of the parent. Now that we
         * removed the downlink for this page, the right sibling might now be
         * the only child of the parent, and could be removed. It would be
         * picked up by the next vacuum anyway, but might as well try to
         * remove it now, so loop back to process the right sibling.
         */
        if (!rightsib_empty) {
            break;
        }

        buf = _bt_getbuf(rel, rightsib, BT_WRITE);
    }

    return ndeleted;
}

/*
 * First stage of page deletion.  Remove the downlink to the top of the
 * branch being deleted, and mark the leaf page as half-dead.
 */
static bool UBTreeMarkPageHalfDead(Relation rel, Buffer leafbuf, BTStack stack)
{
    BlockNumber leafblkno;
    BlockNumber leafrightsib;
    BlockNumber target;
    BlockNumber rightsib;
    ItemId itemid;
    Page page;
    UBTPageOpaqueInternal opaque;
    Buffer topparent;
    OffsetNumber topoff;
    OffsetNumber nextoffset;
    IndexTuple itup;
    IndexTupleData trunctuple;
    errno_t rc;

    WHITEBOX_TEST_STUB("UBTreeMarkPageHalfDead", WhiteboxDefaultErrorEmit);

    page = BufferGetPage(leafbuf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    Assert(!P_RIGHTMOST(opaque) && !P_ISROOT(opaque) && !P_ISDELETED(opaque) && !P_ISHALFDEAD(opaque) &&
           P_ISLEAF(opaque) && P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(page));

    /*
     * Save info about the leaf page.
     */
    leafblkno = BufferGetBlockNumber(leafbuf);
    leafrightsib = opaque->btpo_next;

    /*
     * Before attempting to lock the parent page, check that the right sibling
     * is not in half-dead state.  A half-dead right sibling would have no
     * downlink in the parent, which would be highly confusing later when we
     * delete the downlink that follows the current page's downlink. (I
     * believe the deletion would work correctly, but it would fail the
     * cross-check we make that the following downlink points to the right
     * sibling of the delete page.)
     */
    if (UBTreeIsPageHalfDead(rel, leafrightsib)) {
        elog(DEBUG1, "could not delete page %u because its right sibling %u is half-dead", leafblkno, leafrightsib);
        return false;
    }

    /*
     * We cannot delete a page that is the rightmost child of its immediate
     * parent, unless it is the only child --- in which case the parent has to
     * be deleted too, and the same condition applies recursively to it. We
     * have to check this condition all the way up before trying to delete,
     * and lock the final parent of the to-be-deleted subtree.
     *
     * However, we won't need to repeat the above _bt_is_page_halfdead() check
     * for parent/ancestor pages because of the rightmost restriction. The
     * leaf check will apply to a right "cousin" leaf page rather than a
     * simple right sibling leaf page in cases where we actually go on to
     * perform internal page deletion. The right cousin leaf page is
     * representative of the left edge of the subtree to the right of the
     * to-be-deleted subtree as a whole.  (Besides, internal pages are never
     * marked half-dead, so it isn't even possible to directly assess if an
     * internal page is part of some other to-be-deleted subtree.)
     */
    rightsib = leafrightsib;
    target = leafblkno;
    if (!UBTreeLockBranchParent(rel, leafblkno, stack, &topparent, &topoff, &target, &rightsib)) {
        return false;
    }

    /*
     * Check that the parent-page index items we're about to delete/overwrite
     * contain what we expect.  This can fail if the index has become corrupt
     * for some reason.  We want to throw any error before entering the
     * critical section --- otherwise it'd be a PANIC.
     *
     * The test on the target item is just an Assert because
     * _bt_lock_branch_parent should have guaranteed it has the expected
     * contents.  The test on the next-child downlink is known to sometimes
     * fail in the field, though.
     */
    page = BufferGetPage(topparent);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

#ifdef USE_ASSERT_CHECKING
    itemid = PageGetItemId(page, topoff);
    itup = (IndexTuple) PageGetItem(page, itemid);
    Assert(UBTreeTupleGetDownLink(itup) == target);
#endif

    nextoffset = OffsetNumberNext(topoff);
    itemid = PageGetItemId(page, nextoffset);
    itup = (IndexTuple) PageGetItem(page, itemid);
    if (UBTreeTupleGetDownLink(itup) != rightsib) {
        Buffer rbuf = _bt_getbuf(rel, rightsib, BT_READ);
        Page rpage = BufferGetPage(rbuf);
        UBTPageOpaqueInternal ropaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(rpage);
        if (P_ISHALFDEAD(ropaque)) {
            /* right page already deleted by concurrent worker, do not continue */
            _bt_relbuf(rel, rbuf);
            _bt_relbuf(rel, topparent);
            return false;
        }
        elog(ERROR, "right sibling %u of block %u is not next child %u of block %u in index \"%s\"",
             rightsib, target, UBTreeTupleGetDownLink(itup) != rightsib,
             BufferGetBlockNumber(topparent), RelationGetRelationName(rel));
    }

    /*
     * Any insert which would have gone on the leaf block will now go to its
     * right sibling.
     */
    PredicateLockPageCombine(rel, leafblkno, leafrightsib);

    /* No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    /*
     * Update parent.  The normal case is a tad tricky because we want to
     * delete the target's downlink and the *following* key.  Easiest way is
     * to copy the right sibling's downlink over the target downlink, and then
     * delete the following item.
     */
    page = BufferGetPage(topparent);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    itemid = PageGetItemId(page, topoff);
    itup = (IndexTuple) PageGetItem(page, itemid);
    UBTreeTupleSetDownLink(itup, rightsib);

    nextoffset = OffsetNumberNext(topoff);
    PageIndexTupleDelete(page, nextoffset);

    /*
     * Mark the leaf page as half-dead, and stamp it with a pointer to the
     * highest internal page in the branch we're deleting.  We use the tid of
     * the high key to store it.
     */
    page = BufferGetPage(leafbuf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_flags |= BTP_HALF_DEAD;

    PageIndexTupleDelete(page, P_HIKEY);
    Assert(PageGetMaxOffsetNumber(page) == 0);
    rc = memset_s(&trunctuple, sizeof(IndexTupleData), 0, sizeof(IndexTupleData));
    securec_check(rc, "\0", "\0");
    trunctuple.t_info = sizeof(IndexTupleData);
    if (target != leafblkno) {
        UBTreeTupleSetTopParent(&trunctuple, target);
    } else {
        UBTreeTupleSetTopParent(&trunctuple, InvalidBlockNumber);
    }

    if (PageAddItem(page, (Item)&trunctuple, sizeof(IndexTupleData), P_HIKEY, false, false) == InvalidOffsetNumber) {
        elog(ERROR, "could not add dummy high key to half-dead page");
    }

    /* Must mark buffers dirty before XLogInsert */
    MarkBufferDirty(topparent);
    MarkBufferDirty(leafbuf);

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_btree_mark_page_halfdead xlrec;
        XLogRecPtr recptr;

        xlrec.poffset = topoff;
        xlrec.leafblk = leafblkno;
        if (target != leafblkno) {
            xlrec.topparent = target;
        } else {
            xlrec.topparent = InvalidBlockNumber;
        }

        XLogBeginInsert();
        XLogRegisterBuffer(0, leafbuf, REGBUF_WILL_INIT);
        XLogRegisterBuffer(1, topparent, REGBUF_STANDARD);

        page = BufferGetPage(leafbuf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        xlrec.leftblk = opaque->btpo_prev;
        xlrec.rightblk = opaque->btpo_next;

        XLogRegisterData((char *)&xlrec, SizeOfBtreeMarkPageHalfDead);

        recptr = XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_MARK_PAGE_HALFDEAD);

        page = BufferGetPage(topparent);
        PageSetLSN(page, recptr);
        page = BufferGetPage(leafbuf);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    _bt_relbuf(rel, topparent);
    return true;
}

/*
 * Unlink a page in a branch of half-dead pages from its siblings.
 *
 * If the leaf page still has a downlink pointing to it, unlinks the highest
 * parent in the to-be-deleted branch instead of the leaf page.  To get rid
 * of the whole branch, including the leaf page itself, iterate until the
 * leaf page is deleted.
 *
 * Returns 'false' if the page could not be unlinked (shouldn't happen).
 * If the (new) right sibling of the page is empty, *rightsib_empty is set
 * to true.
 *
 * Must hold pin and lock on leafbuf at entry (read or write doesn't matter).
 * On success exit, we'll be holding pin and write lock.  On failure exit,
 * we'll release both pin and lock before returning (we define it that way
 * to avoid having to reacquire a lock we already released).
 */
static bool UBTreeUnlinkHalfDeadPage(Relation rel, Buffer leafbuf, bool *rightsib_empty)
{
    BlockNumber leafblkno = BufferGetBlockNumber(leafbuf);
    BlockNumber leafleftsib;
    BlockNumber leafrightsib;
    BlockNumber target;
    BlockNumber leftsib;
    BlockNumber rightsib;
    Buffer lbuf = InvalidBuffer;
    Buffer buf;
    Buffer rbuf;
    Buffer metabuf = InvalidBuffer;
    Page metapg = NULL;
    BTMetaPageData *metad = NULL;
    ItemId itemid;
    Page page;
    UBTPageOpaqueInternal opaque;
    bool rightsib_is_rightmost = false;
    int targetlevel;
    IndexTuple leafhikey;
    BlockNumber nextchild;

    WHITEBOX_TEST_STUB("UBTreeUnlinkHalfDeadPage", WhiteboxDefaultErrorEmit);

    page = BufferGetPage(leafbuf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    Assert(P_ISLEAF(opaque) && P_ISHALFDEAD(opaque));

    /*
     * Remember some information about the leaf page.
     */
    itemid = PageGetItemId(page, P_HIKEY);
    leafhikey = (IndexTuple)PageGetItem(page, itemid);
    leafleftsib = opaque->btpo_prev;
    leafrightsib = opaque->btpo_next;

    LockBuffer(leafbuf, BUFFER_LOCK_UNLOCK);

    /*
     * Check here, as calling loops will have locks held, preventing
     * interrupts from being processed.
     */
    CHECK_FOR_INTERRUPTS();

    /*
     * If the leaf page still has a parent pointing to it (or a chain of
     * parents), we don't unlink the leaf page yet, but the topmost remaining
     * parent in the branch.  Set 'target' and 'buf' to reference the page
     * actually being unlinked.
     */
    target = UBTreeTupleGetTopParent(leafhikey);
    if (target != InvalidBlockNumber) {
        Assert(target != leafblkno);

        /* fetch the block number of the topmost parent's left sibling */
        buf = _bt_getbuf(rel, target, BT_READ);
        page = BufferGetPage(buf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        leftsib = opaque->btpo_prev;
        targetlevel = opaque->btpo.level;

        /*
         * To avoid deadlocks, we'd better drop the target page lock before
         * going further.
         */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    } else {
        target = leafblkno;

        buf = leafbuf;
        leftsib = leafleftsib;
        targetlevel = 0;
    }

    /*
     * We have to lock the pages we need to modify in the standard order:
     * moving right, then up.  Else we will deadlock against other writers.
     *
     * So, first lock the leaf page, if it's not the target.  Then find and
     * write-lock the current left sibling of the target page.  The sibling
     * that was current a moment ago could have split, so we may have to move
     * right.  This search could fail if either the sibling or the target page
     * was deleted by someone else meanwhile; if so, give up.  (Right now,
     * that should never happen, since page deletion is only done in VACUUM
     * and there shouldn't be multiple VACUUMs concurrently on the same
     * table.)
     */
    if (target != leafblkno) {
        LockBuffer(leafbuf, BT_WRITE);
    }
    if (leftsib != P_NONE) {
        lbuf = _bt_getbuf(rel, leftsib, BT_WRITE);
        page = BufferGetPage(lbuf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        while (P_ISDELETED(opaque) || opaque->btpo_next != target) {
            /* step right one page */
            leftsib = opaque->btpo_next;
            _bt_relbuf(rel, lbuf);

            /*
             * It'd be good to check for interrupts here, but it's not easy to
             * do so because a lock is always held. This block isn't
             * frequently reached, so hopefully the consequences of not
             * checking interrupts aren't too bad.
             */

            if (leftsib == P_NONE) {
                /* left sibling may deleted concurrently by another backend, which may happen in ubtree */
                if (target != leafblkno) {
                    /* we have only a pin on target, but pin+lock on leafbuf */
                    ReleaseBuffer(buf);
                    _bt_relbuf(rel, leafbuf);
                } else {
                    /* we have only a pin on leafbuf */
                    ReleaseBuffer(leafbuf);
                }
                return false;
            }
            lbuf = _bt_getbuf(rel, leftsib, BT_WRITE);
            page = BufferGetPage(lbuf);
            opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        }
    } else {
        lbuf = InvalidBuffer;
    }

    /*
     * Next write-lock the target page itself.  It should be okay to take just
     * a write lock not a superexclusive lock, since no scans would stop on an
     * empty page.
     */
    LockBuffer(buf, BT_WRITE);
    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    /*
     * Check page is still empty etc, else abandon deletion.  This is just for
     * paranoia's sake; a half-dead page cannot resurrect because there can be
     * only one vacuum process running at a time.
     */
    if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_ISDELETED(opaque)) {
        if (BufferIsValid(lbuf)) {
            _bt_relbuf(rel, lbuf);
        }
        _bt_relbuf(rel, buf);
        return false;
    }
    if (opaque->btpo_prev != leftsib) {
        if (BufferIsValid(lbuf)) {
            _bt_relbuf(rel, lbuf);
        }
        _bt_relbuf(rel, buf);
        return false;
    }

    if (target == leafblkno) {
        if (P_FIRSTDATAKEY(opaque) <= PageGetMaxOffsetNumber(page) || !P_ISLEAF(opaque) || !P_ISHALFDEAD(opaque)) {
            if (BufferIsValid(lbuf)) {
                _bt_relbuf(rel, lbuf);
            }
            _bt_relbuf(rel, buf);
            return false;
        }
        nextchild = InvalidBlockNumber;
    } else {
        if (P_FIRSTDATAKEY(opaque) != PageGetMaxOffsetNumber(page) || P_ISLEAF(opaque)) {
            if (BufferIsValid(lbuf)) {
                _bt_relbuf(rel, lbuf);
            }
            _bt_relbuf(rel, buf);
            return false;
        }

        /* remember the next non-leaf child down in the branch. */
        itemid = PageGetItemId(page, P_FIRSTDATAKEY(opaque));
        nextchild = UBTreeTupleGetDownLink((IndexTuple) PageGetItem(page, itemid));
        if (nextchild == leafblkno) {
            nextchild = InvalidBlockNumber;
        }
    }

    /*
     * And next write-lock the (current) right sibling.
     */
    rightsib = opaque->btpo_next;
    rbuf = _bt_getbuf(rel, rightsib, BT_WRITE);
    page = BufferGetPage(rbuf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (opaque->btpo_prev != target) {
        elog(ERROR,
             "right sibling's left-link doesn't match: "
             "block %u links to %u instead of expected %u in index \"%s\"",
             rightsib, opaque->btpo_prev, target, RelationGetRelationName(rel));
    }
    rightsib_is_rightmost = P_RIGHTMOST(opaque);
    *rightsib_empty = (P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(page));

    /*
     * If we are deleting the next-to-last page on the target's level, then
     * the rightsib is a candidate to become the new fast root. (In theory, it
     * might be possible to push the fast root even further down, but the odds
     * of doing so are slim, and the locking considerations daunting.)
     *
     * We don't support handling this in the case where the parent is becoming
     * half-dead, even though it theoretically could occur.
     *
     * We can safely acquire a lock on the metapage here --- see comments for
     * _bt_newroot().
     */
    if (leftsib == P_NONE && rightsib_is_rightmost) {
        page = BufferGetPage(rbuf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        if (P_RIGHTMOST(opaque)) {
            /* rightsib will be the only one left on the level */
            metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
            metapg = BufferGetPage(metabuf);
            metad = BTPageGetMeta(metapg);
            /*
             * The expected case here is btm_fastlevel == targetlevel+1; if
             * the fastlevel is <= targetlevel, something is wrong, and we
             * choose to overwrite it to fix it.
             */
            if (metad->btm_fastlevel > (uint32)targetlevel + 1) {
                /* no update wanted */
                _bt_relbuf(rel, metabuf);
                metabuf = InvalidBuffer;
            }
        }
    }

    /*
     * Here we begin doing the deletion.
     */

    /* No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    /*
     * Update siblings' side-links.  Note the target page's side-links will
     * continue to point to the siblings.  Asserts here are just rechecking
     * things we already verified above.
     */
    if (BufferIsValid(lbuf)) {
        page = BufferGetPage(lbuf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        Assert(opaque->btpo_next == target);
        opaque->btpo_next = rightsib;
    }
    page = BufferGetPage(rbuf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    Assert(opaque->btpo_prev == target);
    opaque->btpo_prev = leftsib;

    /*
     * If we deleted a parent of the targeted leaf page, instead of the leaf
     * itself, update the leaf to point to the next remaining child in the
     * branch.
     */
    if (target != leafblkno) {
        if (nextchild == leafblkno) {
            UBTreeTupleSetTopParent(leafhikey, InvalidBlockNumber);
        } else {
            UBTreeTupleSetTopParent(leafhikey, nextchild);
        }
    }

    /*
     * Mark the page itself deleted.  It can be recycled when all current
     * transactions are gone.  Storing GetTopTransactionId() would work, but
     * we're in VACUUM and would not otherwise have an XID.  Having already
     * updated links to the target, ReadNewTransactionId() suffices as an
     * upper bound.  Any scan having retained a now-stale link is advertising
     * in its PGXACT an xmin less than or equal to the value we read here.  It
     * will continue to do so, holding back RecentGlobalXmin, for the duration
     * of that scan.
     */
    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_flags &= ~BTP_HALF_DEAD;
    opaque->btpo_flags |= BTP_DELETED;
    opaque->btpo.xact_old = ReadNewTransactionId();

    /* And update the metapage, if needed */
    if (BufferIsValid(metabuf)) {
        metad->btm_fastroot = rightsib;
        metad->btm_fastlevel = targetlevel;
        MarkBufferDirty(metabuf);
    }

    /* Must mark buffers dirty before XLogInsert */
    MarkBufferDirty(rbuf);
    MarkBufferDirty(buf);
    if (BufferIsValid(lbuf)) {
        MarkBufferDirty(lbuf);
    }
    if (target != leafblkno) {
        MarkBufferDirty(leafbuf);
    }

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_btree_unlink_page xlrec;
        xl_btree_metadata xlmeta;
        uint8 xlinfo;
        XLogRecPtr recptr;

        XLogBeginInsert();

        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
        if (BufferIsValid(lbuf))
            XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
        XLogRegisterBuffer(2, rbuf, REGBUF_STANDARD);
        if (target != leafblkno)
            XLogRegisterBuffer(3, leafbuf, REGBUF_WILL_INIT);

        /* information on the unlinked block */
        xlrec.leftsib = leftsib;
        xlrec.rightsib = rightsib;
        xlrec.btpo_xact = opaque->btpo.xact_old;

        /* information needed to recreate the leaf block (if not the target) */
        xlrec.leafleftsib = leafleftsib;
        xlrec.leafrightsib = leafrightsib;
        xlrec.topparent = nextchild;

        XLogRegisterData((char *)&xlrec, SizeOfBtreeUnlinkPage);

        if (BufferIsValid(metabuf)) {
            XLogRegisterBuffer(4, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

            xlmeta.root = metad->btm_root;
            xlmeta.level = metad->btm_level;
            xlmeta.fastroot = metad->btm_fastroot;
            xlmeta.fastlevel = metad->btm_fastlevel;

            XLogRegisterBufData(4, (char *)&xlmeta, sizeof(xl_btree_metadata));
            xlinfo = XLOG_UBTREE_UNLINK_PAGE_META;
        } else {
            xlinfo = XLOG_UBTREE_UNLINK_PAGE;
        }

        recptr = XLogInsert(RM_UBTREE_ID, xlinfo);

        if (BufferIsValid(metabuf)) {
            PageSetLSN(metapg, recptr);
        }
        page = BufferGetPage(rbuf);
        PageSetLSN(page, recptr);
        page = BufferGetPage(buf);
        PageSetLSN(page, recptr);
        if (BufferIsValid(lbuf)) {
            page = BufferGetPage(lbuf);
            PageSetLSN(page, recptr);
        }
        if (target != leafblkno) {
            page = BufferGetPage(leafbuf);
            PageSetLSN(page, recptr);
        }
    }

    END_CRIT_SECTION();

    /* release metapage */
    if (BufferIsValid(metabuf)) {
        _bt_relbuf(rel, metabuf);
    }

    /* release siblings */
    if (BufferIsValid(lbuf)) {
        _bt_relbuf(rel, lbuf);
    }
    _bt_relbuf(rel, rbuf);

    /*
     * Release the target, if it was not the leaf block.  The leaf is always
     * kept locked.
     */
    if (target != leafblkno) {
        _bt_relbuf(rel, buf);
    }

    return true;
}

/*
 *	UBTreeGetNewPage() -- Allocate a new page.
 *
 *		This routine will allocate a new page from Recycle Queue or extend the
 *		relation.
 *
 *		We will try to found a free page from the freed fork of Recycle Queue, and
 *		extend the relation when there is no free page in Recycle Queue.
 *
 *		addr is a output parameter, it will be set to a valid value if the page
 *		is found from Recycle Queue. This output tells where is the corresponding
 *		page in the Recycle Queue, and we need to call UBTreeRecordUsedPage()
 *		with this addr when the returned page is used correctly.
 */
Buffer UBTreeGetNewPage(Relation rel, UBTRecycleQueueAddress* addr)
{
    WHITEBOX_TEST_STUB("UBTreeGetNewPage-begin", WhiteboxDefaultErrorEmit);
restart:
    Buffer buf = UBTreeGetAvailablePage(rel, RECYCLE_FREED_FORK, addr);
    if (buf == InvalidBuffer) {
        /*
         * No free page left, need to extend the relation
         *
         * Extend the relation by one page.
         *
         * We have to use a lock to ensure no one else is extending the rel at
         * the same time, else we will both try to initialize the same new
         * page.  We can skip locking for new or temp relations, however,
         * since no one else could be accessing them.
         */
        bool needLock = !RELATION_IS_LOCAL(rel);
        if (needLock) {
            if (!ConditionalLockRelationForExtension(rel, ExclusiveLock)) {
                /* couldn't get the lock immediately; wait for it. */
                LockRelationForExtension(rel, ExclusiveLock);
                /* check again, relation may extended by other backends */
                buf = UBTreeGetAvailablePage(rel, RECYCLE_FREED_FORK, addr);
                if (buf != InvalidBuffer) {
                    UnlockRelationForExtension(rel, ExclusiveLock);
                    goto out;
                }
                /* Time to bulk-extend. */
                RelationAddExtraBlocks(rel, NULL);
                WHITEBOX_TEST_STUB("UBTreeGetNewPage-bulk-extend", WhiteboxDefaultErrorEmit);
            }
        }
        /* extend by one page */
        buf = ReadBuffer(rel, P_NEW);
        WHITEBOX_TEST_STUB("UBTreeGetNewPage-extend", WhiteboxDefaultErrorEmit);
        if (!ConditionalLockBuffer(buf)) {
            /* lock failed. To avoid dead lock, we need to retry */
            if (needLock) {
                UnlockRelationForExtension(rel, ExclusiveLock);
            }
            ReleaseBuffer(buf);
            goto restart;
        }
        /*
         * Release the file-extension lock; it's now OK for someone else to
         * extend the relation some more.
         */
        if (needLock)
            UnlockRelationForExtension(rel, ExclusiveLock);

        /* we have successfully extended the space, get the new page and write lock */
        addr->queueBuf = InvalidBuffer; /* not allocated from recycle */
    }
out:
    /* buffer is valid, exclusive lock already acquired */
    Assert(BufferIsValid(buf));

    Page page = BufferGetPage(buf);
    if (!_bt_page_recyclable(page)) {
        /* oops, failure due to concurrency, retry. */
        UnlockReleaseBuffer(buf);
        goto restart;
    }

    if (addr->queueBuf != InvalidBuffer) {
        /*
         * If we are generating WAL for Hot Standby then create a
         * WAL record that will allow us to conflict with queries
         * running on standby.
         */
        if (XLogStandbyInfoActive() && RelationNeedsWAL(rel)) {
            UBTPageOpaque opaque = (UBTPageOpaque)PageGetSpecialPointer(page);
            UBTreeLogReusePage(rel, BufferGetBlockNumber(buf), opaque->xact);
        }
    }
    UBTreePageInit(page, BufferGetPageSize(buf));
    return buf;
}

/*
 * Log the reuse of a page from the recycle queue.
 */
static void UBTreeLogReusePage(Relation rel, BlockNumber blkno, TransactionId latestRemovedXid)
{
    xl_btree_reuse_page xlrec;

    if (!RelationNeedsWAL(rel))
        return;

    /*
     * Note that we don't register the buffer with the record, because this
     * operation doesn't modify the page. This record only exists to provide a
     * conflict point for Hot Standby.
     *
     * XLOG stuff
     */
    RelFileNodeRelCopy(xlrec.node, rel->rd_node);

    xlrec.block = blkno;
    xlrec.latestRemovedXid = latestRemovedXid;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfBtreeReusePage);

    (void)XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_REUSE_PAGE, rel->rd_node.bucketNode);
}
