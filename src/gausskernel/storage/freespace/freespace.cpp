/* -------------------------------------------------------------------------
 *
 * freespace.cpp
 *	  openGauss free space map for quickly finding free space in relations
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/freespace/freespace.cpp
 *
 *
 * NOTES:
 *
 *	Free Space Map keeps track of the amount of free space on pages, and
 *	allows quickly searching for a page with enough free space. The FSM is
 *	stored in a dedicated relation fork of all heap relations, and those
 *	index access methods that need it (see also indexfsm.c). See README for
 *	more information.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/htup.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/freespace.h"
#include "storage/fsm_internals.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "commands/tablespace.h"
#include "utils/aiomem.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/storage_gstrace.h"


/* Address of the root page. */
static const FSMAddress g_fsm_root_address = {FSM_ROOT_LEVEL, 0};

/* functions to navigate the tree */
static FSMAddress fsm_get_child(const FSMAddress& parent, uint16 slot);
static FSMAddress fsm_get_parent(const FSMAddress& child, uint16* slot);
static BlockNumber fsm_get_heap_blk(const FSMAddress& addr, uint16 slot);

static Buffer fsm_readbuf(Relation rel, const FSMAddress& addr, bool extend);
static void fsm_extend(Relation rel, BlockNumber fsm_nblocks);

/* functions to convert amount of free space to a FSM category */
static uint8 fsm_space_needed_to_cat(Size needed);
static Size fsm_space_cat_to_avail(uint8 cat);

/* workhorse functions for various operations */
static int fsm_set_and_search(Relation rel, const FSMAddress &addr, uint16 slot, uint8 newValue, uint8 minValue,
    bool search = true);
static BlockNumber fsm_search(Relation rel, uint8 min_cat);
static uint8 fsm_vacuum_page(Relation rel, const FSMAddress& addr, bool* eof);
static BlockNumber fsm_get_lastblckno(Relation rel, const FSMAddress& addr);
static void fsm_update_recursive(Relation rel, const FSMAddress& addr, uint8 new_cat, bool search = true);

/* ******* Public API ******* */
/*
 * GetPageWithFreeSpace - try to find a page in the given relation with
 *		at least the specified amount of free space.
 *
 * If successful, return the block number; if not, return InvalidBlockNumber.
 *
 * The caller must be prepared for the possibility that the returned page
 * will turn out to have too little space available by the time the caller
 * gets a lock on it.  In that case, the caller should report the actual
 * amount of free space available on that page and then try again (see
 * RecordAndGetPageWithFreeSpace).	If InvalidBlockNumber is returned,
 * extend the relation.
 */
BlockNumber GetPageWithFreeSpace(Relation rel, Size spaceNeeded)
{
    uint8 min_cat = fsm_space_needed_to_cat(spaceNeeded);

    return fsm_search(rel, min_cat);
}

/*
 * RecordAndGetPageWithFreeSpace - update info about a page and try again.
 *
 * We provide this combo form to save some locking overhead, compared to
 * separate RecordPageWithFreeSpace + GetPageWithFreeSpace calls. There's
 * also some effort to return a page close to the old page; if there's a
 * page with enough free space on the same FSM page where the old one page
 * is located, it is preferred.
 */
BlockNumber RecordAndGetPageWithFreeSpace(Relation rel, BlockNumber oldPage, Size oldSpaceAvail, Size spaceNeeded)
{
    int old_cat = fsm_space_avail_to_cat(oldSpaceAvail);
    int search_cat = fsm_space_needed_to_cat(spaceNeeded);
    FSMAddress addr;
    uint16 slot;
    int search_slot;

    /* Get the location of the FSM byte representing the heap block */
    addr = fsm_get_location(oldPage, &slot);

    search_slot = fsm_set_and_search(rel, addr, slot, (uint8)old_cat, (uint8)search_cat);
    /*
     * If fsm_set_and_search found a suitable new block, return that.
     * Otherwise, search as usual.
     */
    if (search_slot != -1)
        return fsm_get_heap_blk(addr, (uint16)search_slot);
    else
        return fsm_search(rel, (uint8)search_cat);
}

/*
 * RecordPageWithFreeSpace - update info about a page.
 *
 * Note that if the new spaceAvail value is higher than the old value stored
 * in the FSM, the space might not become visible to searchers until the next
 * FreeSpaceMapVacuum call, which updates the upper level pages.
 */
void RecordPageWithFreeSpace(Relation rel, BlockNumber heapBlk, Size spaceAvail)
{
    int new_cat = fsm_space_avail_to_cat(spaceAvail);
    FSMAddress addr;
    uint16 slot;

    /* Get the location of the FSM byte representing the heap block */
    addr = fsm_get_location(heapBlk, &slot);

    fsm_set_and_search(rel, addr, slot, (uint8)new_cat, 0);
}

/*
 * Update the upper levels of the free space map all the way up to the root
 * to make sure we don't lose track of new blocks we just inserted.  This is
 * intended to be used after adding many new blocks to the relation; we judge
 * it not worth updating the upper levels of the tree every time data for
 * a single page changes, but for a bulk-extend it's worth it.
 */
void UpdateFreeSpaceMap(Relation rel, BlockNumber startBlkNum, BlockNumber endBlkNum, Size freespace, bool search)
{
    int new_cat = fsm_space_avail_to_cat(freespace);
    FSMAddress addr;
    uint16 slot;
    BlockNumber blockNum;
    BlockNumber lastBlkOnPage;

    blockNum = startBlkNum;

    while (blockNum <= endBlkNum) {
        /*
         * Find FSM address for this block; update tree all the way to the
         * root.
         */
        addr = fsm_get_location(blockNum, &slot);
        fsm_update_recursive(rel, addr, (uint8)new_cat, search);

        /*
         * Get the last block number on this FSM page.  If that's greater
         * than or equal to our endBlkNum, we're done.  Otherwise, advance
         * to the first block on the next page.
         */
        lastBlkOnPage = fsm_get_lastblckno(rel, addr);
        if (lastBlkOnPage >= endBlkNum)
            break;
        blockNum = lastBlkOnPage + 1;
    }
}

/*
 * XLogRecordPageWithFreeSpace - like RecordPageWithFreeSpace, for use in
 *		WAL replay
 */
void XLogRecordPageWithFreeSpace(const RelFileNode& rnode, BlockNumber heapBlk, Size spaceAvail)
{
    /*
     * FSM can not be read by physical location in recovery. It is possible to write on wrong places
     * if the FSM fork is dropped and then allocated when replaying old xlog.
     * Since FSM does not have to be totally accurate anyway, just skip it.
     */
    if (IsSegmentFileNode(rnode)) {
        return;
    }

    int new_cat = fsm_space_avail_to_cat(spaceAvail);
    FSMAddress addr;
    uint16 slot;
    BlockNumber blkno;
    Buffer buf;
    Page page;

    /* Get the location of the FSM byte representing the heap block */
    addr = fsm_get_location(heapBlk, &slot);
    blkno = fsm_logical_to_physical(addr);

    /* If the page doesn't exist already, extend */
    buf = XLogReadBufferExtended(rnode, FSM_FORKNUM, blkno, RBM_ZERO_ON_ERROR, NULL);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(buf);
    if (PageIsNew(page))
        PageInit(page, BLCKSZ, 0);

    if (fsm_set_avail(page, (int)slot, (uint8)new_cat))
        MarkBufferDirtyHint(buf, false);
    UnlockReleaseBuffer(buf);
}

/*
 * GetRecordedFreePage - return the amount of free space on a particular page,
 *		according to the FSM.
 */
Size GetRecordedFreeSpace(Relation rel, BlockNumber heapBlk)
{
    FSMAddress addr;
    uint16 slot;
    Buffer buf;
    uint8 cat;

    /* Get the location of the FSM byte representing the heap block */
    addr = fsm_get_location(heapBlk, &slot);

    buf = fsm_readbuf(rel, addr, false);
    if (!BufferIsValid(buf))
        return 0;
    cat = fsm_get_avail(BufferGetPage(buf), slot);
    ReleaseBuffer(buf);

    return fsm_space_cat_to_avail(cat);
}

void XLogBlockTruncateRelFSM(Relation rel, BlockNumber nblocks)
{
    BlockNumber new_nfsmblocks;
    FSMAddress first_removed_address;
    uint16 first_removed_slot;

    RelationOpenSmgr(rel);

    if (!smgrexists(rel->rd_smgr, FSM_FORKNUM))
        return;

    first_removed_address = fsm_get_location(nblocks, &first_removed_slot);

    if (first_removed_slot > 0) {
        new_nfsmblocks = fsm_logical_to_physical(first_removed_address) + 1;
    } else {
        new_nfsmblocks = fsm_logical_to_physical(first_removed_address);
        if (smgrnblocks(rel->rd_smgr, FSM_FORKNUM) <= new_nfsmblocks)
            return; /* nothing to do; the FSM was already smaller */
    }
    smgrtruncatefunc(rel->rd_smgr, FSM_FORKNUM, new_nfsmblocks);
    XLogTruncateRelation(rel->rd_node, MAIN_FORKNUM, new_nfsmblocks);
   
    rel->rd_smgr->smgr_fsm_nblocks = new_nfsmblocks;
}


/*
 * FreeSpaceMapTruncateRel - adjust for truncation of a relation.
 *
 * The caller must hold AccessExclusiveLock on the relation, to ensure that
 * other backends receive the smgr invalidation event that this function sends
 * before they access the FSM again.
 *
 * nblocks is the new size of the heap.
 */
void FreeSpaceMapTruncateRel(Relation rel, BlockNumber nblocks)
{
    BlockNumber new_nfsmblocks;
    FSMAddress first_removed_address;
    uint16 first_removed_slot;
    Buffer buf;

    RelationOpenSmgr(rel);

    /*
     * If no FSM has been created yet for this relation, there's nothing to
     * truncate.
     */
    if (!smgrexists(rel->rd_smgr, FSM_FORKNUM))
        return;

    /* Get the location in the FSM of the first removed heap block */
    first_removed_address = fsm_get_location(nblocks, &first_removed_slot);

    /*
     * Zero out the tail of the last remaining FSM page. If the slot
     * representing the first removed heap block is at a page boundary, as the
     * first slot on the FSM page that first_removed_address points to, we can
     * just truncate that page altogether.
     */
    if (first_removed_slot > 0) {
        buf = fsm_readbuf(rel, first_removed_address, false);
        if (!BufferIsValid(buf))
            return; /* nothing to do; the FSM was already smaller */
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        /* NO EREPORT(ERROR) from here till changes are logged */
        START_CRIT_SECTION();

        fsm_truncate_avail(BufferGetPage(buf), (int)first_removed_slot);

        /*
         * Truncation of a relation is WAL-logged at a higher-level, and we
         * will be called at WAL replay. But if checksums are enabled, we need
         * to still write a WAL record to protect against a torn page, if the
         * page is flushed to disk before the truncation WAL record. We cannot
         * use MarkBufferDirtyHint here, because that will not dirty the page
         * during recovery.
         */
        MarkBufferDirty(buf);
        if (!t_thrd.xlog_cxt.InRecovery && RelationNeedsWAL(rel) && XLogHintBitIsNeeded()) {
            log_newpage_buffer(buf, false);
        }
        END_CRIT_SECTION();

        UnlockReleaseBuffer(buf);

        new_nfsmblocks = fsm_logical_to_physical(first_removed_address) + 1;
    } else {
        new_nfsmblocks = fsm_logical_to_physical(first_removed_address);
        if (smgrnblocks(rel->rd_smgr, FSM_FORKNUM) <= new_nfsmblocks)
            return; /* nothing to do; the FSM was already smaller */
    }

    /* Truncate the unused FSM pages, and send smgr inval message */
    smgrtruncate(rel->rd_smgr, FSM_FORKNUM, new_nfsmblocks);

    /*
     * We might as well update the local smgr_fsm_nblocks setting.
     * smgrtruncate sent an smgr cache inval message, which will cause other
     * backends to invalidate their copy of smgr_fsm_nblocks, and this one too
     * at the next command boundary.  But this ensures it isn't outright wrong
     * until then.
     */
    rel->rd_smgr->smgr_fsm_nblocks = new_nfsmblocks;
}

/*
 * FreeSpaceMapVacuum - scan and fix any inconsistencies in the FSM
 */
void FreeSpaceMapVacuum(Relation rel)
{
    bool dummy = false;

    /*
     * Traverse the tree in depth-first order. The tree is stored physically
     * in depth-first order, so this should be pretty I/O efficient.
     */
    fsm_vacuum_page(rel, g_fsm_root_address, &dummy);
}

/* ******* Internal routines ******* */
/*
 * Return category corresponding x bytes of free space
 */
uint8 fsm_space_avail_to_cat(Size avail)
{
    int cat;

    Assert(avail < BLCKSZ);

    if (avail >= MaxFSMRequestSize) {
        return 255;
    }

    cat = avail / FSM_CAT_STEP;

    /*
     * The highest category, 255, is reserved for MaxFSMRequestSize bytes or
     * more.
     */
    if (cat > 254) {
        cat = 254;
    }

    return (uint8)cat;
}

/*
 * Return the lower bound of the range of free space represented by given
 * category.
 */
static Size fsm_space_cat_to_avail(uint8 cat)
{
    /* The highest category represents exactly MaxFSMRequestSize bytes. */
    if (cat == 255)
        return MaxFSMRequestSize;
    else
        return cat * FSM_CAT_STEP;
}

/*
 * Which category does a page need to have, to accommodate x bytes of data?
 * While fsm_size_to_avail_cat() rounds down, this needs to round up.
 */
static uint8 fsm_space_needed_to_cat(Size needed)
{
    int cat;

    /* Can't ask for more space than the highest category represents */
    if (needed > MaxFSMRequestSize) {
        ereport(
            ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("invalid FSM request size %lu", (unsigned long)needed)));
    }

    if (needed == 0) {
        return 1;
    }

    cat = (needed + FSM_CAT_STEP - 1) / FSM_CAT_STEP;

    if (cat > 255) {
        cat = 255;
    }

    return (uint8)cat;
}

/*
 * Returns the physical block number an FSM page
 */
BlockNumber fsm_logical_to_physical(const FSMAddress& addr)
{
    BlockNumber pages;
    int leafno;
    int l;

    /*
     * Calculate the logical page number of the first leaf page below the
     * given page.
     */
    leafno = addr.logpageno;
    for (l = 0; l < addr.level; l++)
        leafno *= SlotsPerFSMPage;

    /* Count upper level nodes required to address the leaf page */
    pages = 0;
    for (l = 0; l < FSM_TREE_DEPTH; l++) {
        pages += (BlockNumber)leafno + 1;
        leafno /= SlotsPerFSMPage;
    }

    /*
     * If the page we were asked for wasn't at the bottom level, subtract the
     * additional lower level pages we counted above.
     */
    pages -= (BlockNumber)addr.level;

    /* Turn the page count into 0-based block number */
    return pages - 1;
}

/*
 * Return the FSM location corresponding to given heap block.
 */
FSMAddress fsm_get_location(BlockNumber heapblk, uint16* slot)
{
    FSMAddress addr;

    addr.level = FSM_BOTTOM_LEVEL;
    addr.logpageno = heapblk / SlotsPerFSMPage;
    *slot = heapblk % SlotsPerFSMPage;

    return addr;
}

/*
 * Return the heap block number corresponding to given location in the FSM.
 */
static BlockNumber fsm_get_heap_blk(const FSMAddress& addr, uint16 slot)
{
    Assert(addr.level == FSM_BOTTOM_LEVEL);
    return ((unsigned int)addr.logpageno) * SlotsPerFSMPage + slot;
}

/*
 * Given a logical address of a child page, get the logical page number of
 * the parent, and the slot within the parent corresponding to the child.
 */
static FSMAddress fsm_get_parent(const FSMAddress& child, uint16* slot)
{
    FSMAddress parent;

    Assert(child.level < FSM_ROOT_LEVEL);

    parent.level = child.level + 1;
    parent.logpageno = child.logpageno / SlotsPerFSMPage;
    *slot = child.logpageno % SlotsPerFSMPage;

    return parent;
}

/*
 * Given a logical address of a parent page, and a slot number get the
 * logical address of the corresponding child page.
 */
static FSMAddress fsm_get_child(const FSMAddress& parent, uint16 slot)
{
    FSMAddress child;

    Assert(parent.level > FSM_BOTTOM_LEVEL);

    child.level = parent.level - 1;
    child.logpageno = parent.logpageno * SlotsPerFSMPage + slot;

    return child;
}

/*
 * Read a FSM page.
 *
 * If the page doesn't exist, InvalidBuffer is returned, or if 'extend' is
 * true, the FSM file is extended.
 */
static Buffer fsm_readbuf(Relation rel, const FSMAddress& addr, bool extend)
{
    BlockNumber blkno = fsm_logical_to_physical(addr);
    Buffer buf;

    RelationOpenSmgr(rel);

    /*
     * If we haven't cached the size of the FSM yet, check it first.  Also
     * recheck if the requested block seems to be past end, since our cached
     * value might be stale.  (We send smgr inval messages on truncation, but
     * not on extension.)
     */
    if (rel->rd_smgr->smgr_fsm_nblocks == InvalidBlockNumber || blkno >= rel->rd_smgr->smgr_fsm_nblocks) {
        if (smgrexists(rel->rd_smgr, FSM_FORKNUM))
            rel->rd_smgr->smgr_fsm_nblocks = smgrnblocks(rel->rd_smgr, FSM_FORKNUM);
        else
            rel->rd_smgr->smgr_fsm_nblocks = 0;
    }

    /* Handle requests beyond EOF */
    if (blkno >= rel->rd_smgr->smgr_fsm_nblocks) {
        if (extend)
            fsm_extend(rel, blkno + 1);
        else
            return InvalidBuffer;
    }

    /*
     * Use ZERO_ON_ERROR mode, and initialize the page if necessary. The FSM
     * information is not accurate anyway, so it's better to clear corrupt
     * pages than error out. Since the FSM changes are not WAL-logged, the
     * so-called torn page problem on crash can lead to pages with corrupt
     * headers, for example.
     *
     * The initialize-the-page part is trickier than it looks, because of the
     * possibility of multiple backends doing this concurrently, and our
     * desire to not uselessly take the buffer lock in the normal path where
     * the page is OK.  We must take the lock to initialize the page, so
     * recheck page newness after we have the lock, in case someone else
     * already did it.  Also, because we initially check PageIsNew with no
     * lock, it's possible to fall through and return the buffer while someone
     * else is still initializing the page (i.e., we might see pd_upper as set
     * but other page header fields are still zeroes).  This is harmless for
     * callers that will take a buffer lock themselves, but some callers
     * inspect the page without any lock at all.  The latter is OK only so
     * long as it doesn't depend on the page header having correct contents.
     * Current usage is safe because PageGetContents() does not require that.
     */
    buf = ReadBufferExtended(rel, FSM_FORKNUM, blkno, RBM_ZERO_ON_ERROR, NULL);
    if (PageIsNew(BufferGetPage(buf))) {
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        if (PageIsNew(BufferGetPage(buf))) {
            PageInit(BufferGetPage(buf), BLCKSZ, 0);
        }
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    }

    return buf;
}

/*
 * Ensure that the FSM fork is at least fsm_nblocks long, extending
 * it if necessary with empty pages. And by empty, I mean pages filled
 * with zeros, meaning there's no free space.
 */
static void fsm_extend(Relation rel, BlockNumber fsm_nblocks)
{
    BlockNumber fsm_nblocks_now;
    Page pg;
    char* unalign_buffer = NULL;

    ADIO_RUN()
    {
        pg = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        if (ENABLE_DSS) {
            unalign_buffer = (char*)palloc(BLCKSZ + ALIGNOF_BUFFER);
            pg = (Page)BUFFERALIGN(unalign_buffer);
        } else {
            pg = (char*)palloc(BLCKSZ);
        }
    }
    ADIO_END();

    PageInit(pg, BLCKSZ, 0);

    /*
     * We use the relation extension lock to lock out other backends trying to
     * extend the FSM at the same time. It also locks out extension of the
     * main fork, unnecessarily, but extending the FSM happens seldom enough
     * that it doesn't seem worthwhile to have a separate lock tag type for
     * it.
     *
     * Note that another backend might have extended or created the relation
     * by the time we get the lock.
     */
    LockRelationForExtension(rel, ExclusiveLock);

    /* Might have to re-open if a cache flush happened */
    RelationOpenSmgr(rel);

    /*
     * Create the FSM file first if it doesn't exist.  If smgr_fsm_nblocks is
     * positive then it must exist, no need for an smgrexists call.
     */
    if ((rel->rd_smgr->smgr_fsm_nblocks == 0 || rel->rd_smgr->smgr_fsm_nblocks == InvalidBlockNumber) &&
        !smgrexists(rel->rd_smgr, FSM_FORKNUM))
        smgrcreate(rel->rd_smgr, FSM_FORKNUM, false);

    fsm_nblocks_now = smgrnblocks(rel->rd_smgr, FSM_FORKNUM);
    // check tablespace size limitation when extending FSM file.
    if (fsm_nblocks_now < fsm_nblocks) {
        STORAGE_SPACE_OPERATION(rel, ((uint64)BLCKSZ) * (fsm_nblocks - fsm_nblocks_now));
        RelationOpenSmgr(rel);
    }

    while (fsm_nblocks_now < fsm_nblocks) {
        if (IsSegmentFileNode(rel->rd_node)) {
            Buffer buf = ReadBufferExtended(rel, FSM_FORKNUM, P_NEW, RBM_ZERO, NULL);
#ifdef USE_ASSERT_CHECKING
            BufferDesc *buf_desc = GetBufferDescriptor(buf - 1);
            Assert(buf_desc->tag.blockNum == fsm_nblocks_now);
#endif
            ReleaseBuffer(buf);
        } else {
            PageSetChecksumInplace(pg, fsm_nblocks_now);

            smgrextend(rel->rd_smgr, FSM_FORKNUM, fsm_nblocks_now, (char*)pg, false);
        }
        fsm_nblocks_now++;
    }

    /* Update local cache with the up-to-date size */
    rel->rd_smgr->smgr_fsm_nblocks = fsm_nblocks_now;

    UnlockRelationForExtension(rel, ExclusiveLock);

    ADIO_RUN()
    {
        adio_align_free(pg);
    }
    ADIO_ELSE()
    {
        if (ENABLE_DSS) {
            pfree(unalign_buffer);
        } else {
            pfree(pg);
        }
    }
    ADIO_END();
}

/*
 * Set value in given FSM page and slot.
 *
 * If minValue > 0, the updated page is also searched for a page with at
 * least minValue of free space. If one is found, its slot number is
 * returned, -1 otherwise.
 */
static int fsm_set_and_search(Relation rel, const FSMAddress &addr, uint16 slot, uint8 newValue, uint8 minValue,
    bool search)
{
    Buffer buf;
    Page page;
    int newslot = -1;

    gstrace_entry(GS_TRC_ID_fsm_set_and_search);

    buf = fsm_readbuf(rel, addr, true);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(buf);
    if (fsm_set_avail(page, slot, newValue))
        MarkBufferDirtyHint(buf, false);

    if (minValue != 0 && search) {
        /* Search while we still hold the lock */
        newslot = fsm_search_avail(buf, minValue, addr.level == FSM_BOTTOM_LEVEL, true);
    }

    UnlockReleaseBuffer(buf);
    gstrace_exit(GS_TRC_ID_fsm_set_and_search);
    return newslot;
}

/*
 * Search the tree for a heap page with at least min_cat of free space
 */
static BlockNumber fsm_search(Relation rel, uint8 min_cat)
{
    int restarts = 0;
    FSMAddress addr = g_fsm_root_address;

    gstrace_entry(GS_TRC_ID_fsm_search);

    for (;;) {
        int slot;
        Buffer buf;
        uint8 max_avail = 0;

        /* Read the FSM page. */
        buf = fsm_readbuf(rel, addr, false);
        /* Search within the page */
        if (BufferIsValid(buf)) {
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            slot = fsm_search_avail(buf, min_cat, (addr.level == FSM_BOTTOM_LEVEL), false);
            if (slot == -1)
                max_avail = fsm_get_max_avail(BufferGetPage(buf));
            UnlockReleaseBuffer(buf);
        } else
            slot = -1;

        if (slot != -1) {
            /*
             * Descend the tree, or return the found block if we're at the
             * bottom.
             */
            if (addr.level == FSM_BOTTOM_LEVEL) {
                gstrace_exit(GS_TRC_ID_fsm_search);
                return fsm_get_heap_blk(addr, (uint16)slot);
            }
            addr = fsm_get_child(addr, (uint16)slot);
        } else if (addr.level == FSM_ROOT_LEVEL) {
            gstrace_exit(GS_TRC_ID_fsm_search);
            /*
             * At the root, failure means there's no page with enough free
             * space in the FSM. Give up.
             */
            return InvalidBlockNumber;
        } else {
            uint16 parentslot;
            FSMAddress parent;

            /*
             * At lower level, failure can happen if the value in the upper-
             * level node didn't reflect the value on the lower page. Update
             * the upper node, to avoid falling into the same trap again, and
             * start over.
             *
             * There's a race condition here, if another backend updates this
             * page right after we release it, and gets the lock on the parent
             * page before us. We'll then update the parent page with the now
             * stale information we had. It's OK, because it should happen
             * rarely, and will be fixed by the next vacuum.
             */
            parent = fsm_get_parent(addr, &parentslot);
            fsm_set_and_search(rel, parent, parentslot, max_avail, 0);

            /*
             * If the upper pages are badly out of date, we might need to loop
             * quite a few times, updating them as we go. Any inconsistencies
             * should eventually be corrected and the loop should end. Looping
             * indefinitely is nevertheless scary, so provide an emergency
             * valve.
             */
            if (restarts++ > 10000) {
                gstrace_exit(GS_TRC_ID_fsm_search);
                return InvalidBlockNumber;
            }

            /* Start search all over from the root */
            addr = g_fsm_root_address;
        }
    }
    gstrace_exit(GS_TRC_ID_fsm_search);
    return InvalidBlockNumber; /* should not reached, avoid compile warning */
}

/*
 * Recursive guts of FreeSpaceMapVacuum
 */
static uint8 fsm_vacuum_page(Relation rel, const FSMAddress& addr, bool* eof_p)
{
    Buffer buf;
    Page page;
    uint8 max_avail;

    gstrace_entry(GS_TRC_ID_fsm_vacuum_page);

    /* Read the page if it exists, or return EOF */
    buf = fsm_readbuf(rel, addr, false);
    if (!BufferIsValid(buf)) {
        *eof_p = true;
        gstrace_exit(GS_TRC_ID_fsm_vacuum_page);
        return 0;
    } else
        *eof_p = false;

    page = BufferGetPage(buf);

    /*
     * Recurse into children, and fix the information stored about them at
     * this level.
     */
    if (addr.level > FSM_BOTTOM_LEVEL) {
        int slot;
        bool eof = false;

        for (slot = 0; (unsigned int)(slot) < SlotsPerFSMPage; slot++) {
            int child_avail;

            CHECK_FOR_INTERRUPTS();

            /* After we hit end-of-file, just clear the rest of the slots */
            if (!eof)
                child_avail = fsm_vacuum_page(rel, fsm_get_child(addr, (uint16)slot), &eof);
            else
                child_avail = 0;

            /* Update information about the child */
            if (fsm_get_avail(page, slot) != child_avail) {
                LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
                fsm_set_avail(page, slot, (uint8)child_avail);
                MarkBufferDirtyHint(buf, false);
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }
        }
    }

    max_avail = fsm_get_max_avail(page);

    /*
     * Reset the next slot pointer. This encourages the use of low-numbered
     * pages, increasing the chances that a later vacuum can truncate the
     * relation.
     */
    ((FSMPage)PageGetContents(page))->fp_next_slot = 0;

    ReleaseBuffer(buf);
    gstrace_exit(GS_TRC_ID_fsm_vacuum_page);
    return max_avail;
}

/*
 * This function will return the last block number stored on given
 * FSM page address.
 */
static BlockNumber fsm_get_lastblckno(Relation rel, const FSMAddress& addr)
{
    int slot;

    /*
     * Get the last slot number on the given address and convert that to
     * block number
     */
    slot = SlotsPerFSMPage - 1;
    return fsm_get_heap_blk(addr, (uint16)slot);
}

/*
 * Recursively update the FSM tree from given address to
 * all the way up to root.
 */
static void fsm_update_recursive(Relation rel, const FSMAddress& addr, uint8 new_cat, bool search)
{
    uint16 parentslot;
    FSMAddress parent;

    if (addr.level == FSM_ROOT_LEVEL)
        return;

    /*
     * Get the parent page and our slot in the parent page, and
     * update the information in that.
     */
    parent = fsm_get_parent(addr, &parentslot);
    fsm_set_and_search(rel, parent, parentslot, new_cat, 0, search);
    fsm_update_recursive(rel, parent, new_cat, search);
}

BlockNumber FreeSpaceMapCalTruncBlkNo(BlockNumber relBlkNo)
{
    BlockNumber new_nfsmblocks;
    FSMAddress first_removed_address;
    uint16 first_removed_slot;

    /* Get the location in the FSM of the first removed heap block */
    first_removed_address = fsm_get_location(relBlkNo, &first_removed_slot);

    new_nfsmblocks = fsm_logical_to_physical(first_removed_address);

    return new_nfsmblocks;
}
