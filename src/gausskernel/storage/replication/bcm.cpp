/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *

 *
 * bcm.cpp
 *	  bcm map for tracking modify of heap blocks
 *
 *
 * IDENTIFICATION
 * 		src/gausskernel/storage/replication/bcm.cpp
 *
 * INTERFACE ROUTINES
 *		createBCMFile		 - create BCM file with a init file header
 *		BCM_pin	     		 - pin a map page for setting a bit
 *		BCMSetStatusBit	     - set Status in a previously pinned page
 *		BCMTestStatusBit     - test if a bit is set
 *		BCMCountStatusBits   - fast count number of bits set in BCM map
 *		BCMTruncateFile 	 - truncate the BCM map
 *		BCMClearRel		 	 - clear all the BCM bis of a rel
 *		getBcmFileList		 -
 *
 * NOTES
 *
 * The bcm map is a bitmap with two bits(one for sync and another for backup)
 * per heap block. A set bit means that block is modified and has not sync to
 * the standby,if a bit is not set, it means the block has sync to standby and
 * no need to be sync.
 *
 * Clearing a bcm map bit is not separately WAL-logged.
 *
 * When a bit is set, the LSN of the bcm map page is updated to make
 * sure that the bcm map update doesn't get written to disk before the
 * WAL record of the changes that made it possible to set the bit is flushed.
 * But when a bit is cleared, we don't have to do that because it's always
 * safe to clear a bit in the map from correctness point of view.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xlogutils.h"
#include "access/visibilitymap.h"
#include "catalog/catalog.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/fd.h"
#include "storage/cu.h"
#include "utils/inval.h"
#include "postmaster/alarmchecker.h"

#include "replication/bcm.h"
#include "replication/basebackup.h"
#include "replication/catchup.h"
#include "replication/dataprotocol.h"
#include "replication/dataqueue.h"
#include "replication/datasender.h"
#include "replication/datasender_private.h"
#include "replication/walsender.h"

#include "utils/aiomem.h"
#include "utils/memutils.h"
#include "storage/custorage.h"
#include "storage/ipc.h"
#include "commands/tablespace.h"

/*
 * Table for fast counting of set bits, by now only for sync
 * FUTURE CASE:: for backup(the second bit)
 *
 * Define bcm postfix
 */
#define BCM "_bcm"

/* prototypes for internal routines */
static Buffer BCM_readbuf(Relation rel, BlockNumber blkno, bool extend, int col = 0);
static void BCM_extend(Relation rel, BlockNumber nvmblocks, int col = 0);
static void searchBCMFiles(const char *tableSpacePath, const char *relativepath, bool undertablespace, bool clear,
                           int iterations);
static void GetIncrementalBcmFilePathForDefault(const RelFileNodeKey &data, char *path, int length);
static void GetIncrementalBcmFilePathForCustome(const RelFileNodeKey &data, char *path, int length);
static void HandleBCMfile(char *bcmpath, bool clear);
static void BCMClearFile(const RelFileNode &relfilenode, int col = 0);
static void BCMSendData(const RelFileNode &relfilenode, const char *bcmpath, int col = 0);
static void bcm_read_multi_cu(CUFile *cFile, Relation rel, int col, BlockNumber heapBlock, int &contibits,
                              BlockNumber maxHeapBlock);
static void BCMSetMetaBit(Relation rel, BlockNumber block, BCMBitStatus status, int col = 0);
static void BCMClearMetaBit(Relation rel, int col = 0);
static void BCMResetMetaBit(Relation rel, BlockNumber metablk, int col = 0);
static void BCMWalkMetaBuffer(Relation rel, CUFile *cFile, Buffer metabuffer, BlockNumber &heapBlock, int &contibits,
                              BlockNumber maxHeapBlock, int col = 0);
static void BCMSendOneBuffer(Relation rel, CUFile *cFile, Buffer bcmbuffer, BlockNumber &heapBlock, int &contibits,
                             BlockNumber maxHeapBlock, int col = 0);
static BlockNumber BCMGetDataFileMaxSize(Relation rel, int col);
static bool CheckFilePostfix(const char *str1, const char *str2);

// check tablespace size limitation when extending BCM file.
static inline void VerifyTblspcWhenBcmExtend(Relation rel, int col, int nblocks)
{
    Assert(nblocks > 0);
    STORAGE_SPACE_OPERATION(rel, (uint64)BLCKSZ * nblocks);

    // Might have to re-open if a cache flush happened
    if (col > 0) {
        CStoreRelationOpenSmgr(rel, col);
    } else {
        RelationOpenSmgr(rel);
    }
}

/* Create a bcm file with an inited bcm file header */
void createBCMFile(Relation rel, int col)
{
    Page bcmHeader;

    ADIO_RUN()
    {
        bcmHeader = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        bcmHeader = (Page)palloc(BLCKSZ);
    }
    ADIO_END();

    PageInit(bcmHeader, BLCKSZ, 0);
    BCMHeader *hd = NULL;
    ForkNumber forknum = BCM_FORKNUM;

    hd = (BCMHeader *)PageGetContents(bcmHeader);

    /* FUTURE CASE:: for COLUMN_STORE, only support ROW_STORE by now. */
    hd->type = col > 0 ? COLUMN_STORE : ROW_STORE;
    hd->node.dbNode = rel->rd_node.dbNode;
    hd->node.relNode = rel->rd_node.relNode;
    hd->node.spcNode = rel->rd_node.spcNode;
    hd->node.bucketNode = rel->rd_node.bucketNode;
    hd->node.opt = rel->rd_node.opt;
    hd->blockSize = col > 0 ? CUAlignUtils::GetCuAlignSizeColumnId(col) : BLCKSZ; /* defaut size for ROW_STORE */

    if (col > 0)
        forknum = ColumnId2ColForkNum(col);

    smgrcreate(rel->rd_smgr, forknum, false);

    VerifyTblspcWhenBcmExtend(rel, col, 1);

    PageSetChecksumInplace(bcmHeader, 0);

    /* Now extend the file */
    smgrextend(rel->rd_smgr, forknum, 0, (char *)bcmHeader, false);

    ADIO_RUN()
    {
        adio_align_free(bcmHeader);
    }
    ADIO_ELSE()
    {
        pfree(bcmHeader);
        bcmHeader = NULL;
    }
    ADIO_END();
}

void BCMLogCU(Relation rel, uint64 offset, int col, BCMBitStatus status, int count)
{
    bool needwal = false;

    needwal = (RelationNeedsWAL(rel) && !t_thrd.xlog_cxt.InRecovery);

    if (needwal) {
        Buffer bcmbuffer = InvalidBuffer;
        Page page;

        BCM_CStore_pin(rel, col, offset, &bcmbuffer);
        LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(bcmbuffer);

        START_CRIT_SECTION();
        {
            uint64 cuBlock = 0;
            XLogRecPtr recptr = InvalidXLogRecPtr;
            uint64 align_size = (uint64)(uint32)CUAlignUtils::GetCuAlignSizeColumnId(col);
            cuBlock = cstore_offset_to_cstoreblock(offset, align_size);
            recptr = log_cu_bcm(&(rel->rd_node), col, cuBlock, status, count);
            PageSetLSN(page, recptr);
        }
        END_CRIT_SECTION();

        LockBuffer(bcmbuffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(bcmbuffer);
    }
}

/*
 * Set the meta page sync bit where the bcm block refers to.
 * Here, we use two sync bits(sync bit 0 and sync bit 1) to represent
 * the sync status of a bcm page. When we set sync bit 1 to unsynced, it
 * means the bcm page may have unsynced heap blocks. sync. When we set
 * sync bit 0 to unsynced during catchup, it means that the bcm page status
 * bit in meta page should not be reset after catchup which can be perceived
 * by the next catchup.
 * Note: call the function should first hold BUFFER_LOCK_EXCLUSIVE lock
 */
static void BCMSetMetaBit(Relation rel, BlockNumber block, BCMBitStatus status, int col)
{
    BlockNumber metablock = BCMBLK_TO_METABLOCK(block);
    int metaByte = BCMBLK_TO_METABYTE(block);
    int metaBit = BCMBLK_TO_METABIT(block);
    uint32 bshift = (uint32)metaBit * META_BITS_PER_BLOCK;
    Buffer metabuffer = InvalidBuffer;
    BCMBitStatus pageStatus0 = 0;
    BCMBitStatus pageStatus1 = 0;
    Page page;
    unsigned char *map = NULL;

    Assert(status == SYNCED || status == NOTSYNCED);

    metabuffer = BCM_readbuf(rel, metablock, false, col);
    Assert(BufferIsValid(metabuffer));
    LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(metabuffer);
    map = (unsigned char *)PageGetContents(page);

    /* get sync bit 0 & 1 status */
    pageStatus0 = ((map[metaByte] >> bshift) & META_SYNC0_BITMASK) >> 3;
    Assert(pageStatus0 == SYNCED || pageStatus0 == NOTSYNCED);
    pageStatus1 = ((map[metaByte] >> bshift) & META_SYNC1_BITMASK) >> 1;
    Assert(pageStatus1 == SYNCED || pageStatus1 == NOTSYNCED);

    /* set sync bit status */
    if (status != pageStatus0 || status != pageStatus1) {
        START_CRIT_SECTION();
        if (status != pageStatus0)
            SET_SYNC0_BYTE_STATUS(map[metaByte], status, bshift);
        if (status != pageStatus1)
            SET_SYNC1_BYTE_STATUS(map[metaByte], status, bshift);
        MarkBufferDirty(metabuffer);
        END_CRIT_SECTION();
    }

    UnlockReleaseBuffer(metabuffer);
}

/*
 * Clear all bcm page sync status bit 0 in meta pages before catchup.
 */
static void BCMClearMetaBit(Relation rel, int col)
{
    BlockNumber metablock = 1;
    Buffer metabuffer = InvalidBuffer;
    Page page;
    unsigned char *map = NULL;
    uint32 bshift = 0;
    BCMBitStatus pageStatus0 = 0;
    int i = 0;
    int j = 0;
    bool dirty = false;

    metabuffer = BCM_readbuf(rel, metablock, false, col);
    if (!BufferIsValid(metabuffer))
        return; /* nothing to */

    do {
        LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(metabuffer);
        map = (unsigned char *)PageGetContents(page);

        ereport(DEBUG1, (errmsg("relation %u/%u/%u col %d try to clear meta block %u", rel->rd_node.spcNode,
                                rel->rd_node.dbNode, rel->rd_node.relNode, col, metablock)));

        /* clear sync bit 0 status */
        START_CRIT_SECTION();
        for (i = 0; i < (int)BCMMAPSIZE; i++) {
            for (j = 0; j < META_BLOCKS_PER_BYTE; j++) {
                bshift = (uint32)j * META_BITS_PER_BLOCK;
                pageStatus0 = ((map[i] >> bshift) & META_SYNC0_BITMASK) >> 3;
                Assert(pageStatus0 == SYNCED || pageStatus0 == NOTSYNCED);
                if (pageStatus0 == NOTSYNCED) {
                    SET_SYNC0_BYTE_STATUS(map[i], SYNCED, bshift);
                    dirty = true;
                }
            }
        }
        if (dirty)
            MarkBufferDirty(metabuffer);
        END_CRIT_SECTION();

        UnlockReleaseBuffer(metabuffer);

        /* caculate the next meta page, than clear again. */
        metablock += META_BLOCKS_PER_PAGE + 1;
        metabuffer = BCM_readbuf(rel, metablock, false, col);
    } while (BufferIsValid(metabuffer));

    return;
}

/*
 * Reset the bcm page sync status bit 1 in meta pages after catchup.
 * Notes: we skip those bcm pages which are recently marked as unsynced
 * by checking the sync status bit 0.
 */
static void BCMResetMetaBit(Relation rel, BlockNumber metablk, int col)
{
    BlockNumber metablock = 1;
    Buffer metabuffer = InvalidBuffer;
    Page page;
    unsigned char *map = NULL;
    uint32 bshift = 0;
    BCMBitStatus pageStatus0 = 0;
    BCMBitStatus pageStatus1 = 0;
    int i = 0;
    int j = 0;
    bool dirty = false;

    for (metablock = 1; metablock < metablk; metablock += (META_BLOCKS_PER_PAGE + 1)) {
        metabuffer = BCM_readbuf(rel, metablock, false, col);
        if (!BufferIsValid(metabuffer))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("%u/%u/%u invalid bcm meta buffer %u", rel->rd_node.spcNode, rel->rd_node.dbNode,
                                   rel->rd_node.relNode, metablock)));

        LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(metabuffer);
        map = (unsigned char *)PageGetContents(page);

        ereport(DEBUG1, (errmsg("relation %u/%u/%u col %d try to reset meta block %u", rel->rd_node.spcNode,
                                rel->rd_node.dbNode, rel->rd_node.relNode, col, metablock)));
        /*
         * Clear the latest set sync bit 1 status, the the page status 0 has been set
         * to NOTSYNCED sync last meta clear, we should skip this BCM block.
         */
        START_CRIT_SECTION();
        for (i = 0; i < (int)BCMMAPSIZE; i++) {
            for (j = 0; j < META_BLOCKS_PER_BYTE; j++) {
                bshift = (uint32)j * META_BITS_PER_BLOCK;
                pageStatus0 = ((map[i] >> bshift) & META_SYNC0_BITMASK) >> 3;
                Assert(SYNCED == pageStatus0 || NOTSYNCED == pageStatus0);
                pageStatus1 = ((map[i] >> bshift) & META_SYNC1_BITMASK) >> 1;
                Assert(SYNCED == pageStatus1 || NOTSYNCED == pageStatus1);
                if (SYNCED == pageStatus0 && NOTSYNCED == pageStatus1) {
                    SET_SYNC1_BYTE_STATUS(map[i], SYNCED, bshift);
                    dirty = true;
                }
            }
        }
        if (dirty)
            MarkBufferDirty(metabuffer);
        END_CRIT_SECTION();

        UnlockReleaseBuffer(metabuffer);
    }

    return;
}

/*
 * Set the corresponding bit of the heap block as status, before call
 * this function we should call BCM_pin to get the right bcmbuffer.
 */
void BCMSetStatusBit(Relation rel, uint64 heapBlk, Buffer buf, BCMBitStatus status, int col)
{
    BlockNumber mapBlock = HEAPBLK_TO_BCMBLOCK(heapBlk);
    int mapByte = HEAPBLK_TO_BCMBYTE(heapBlk);
    int mapBit = HEAPBLK_TO_BCMBIT(heapBlk);
    uint32 bshift = (uint32)mapBit * BCM_BITS_PER_BLOCK;
    BCMBitStatus bcmStatus = 0;
    bool needwal = false;
    Page page;
    unsigned char *map = NULL;

#ifdef TRACE_BCMMAP
    elog(LOG, "BCMSetStatusBit: rel: %s col: %d blk: %lu  status: %d ", RelationGetRelationName(rel), col, heapBlk,
         status);
#endif

    if (!BufferIsValid(buf) || BufferGetBlockNumber(buf) != mapBlock)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED), errmsg("wrong buffer passed to BCM_clear, BlockNumber from buf is %u,"
                                                         "mapBlock is %u",
                                                         BufferGetBlockNumber(buf), mapBlock)));

    Assert(status == SYNCED || status == NOTSYNCED);

    if (status == NOTSYNCED)
        BCMSetMetaBit(rel, mapBlock, NOTSYNCED, col);

    page = BufferGetPage(buf);
    map = (unsigned char *)PageGetContents(page);

    bcmStatus = (map[mapByte] >> bshift) & BCM_SYNC_BITMASK;
    bcmStatus = bcmStatus >> 1;
    Assert(bcmStatus == SYNCED || bcmStatus == NOTSYNCED);

    /* Bcm status must be 0 before it will be set to 1 */
    if (!RecoveryInProgress() && status == NOTSYNCED && bcmStatus == NOTSYNCED)
        ereport(WARNING, (errmsg("BCM page maybe damage, rnode[%u,%u,%u] col:%d block:%lu ", rel->rd_node.spcNode,
                                 rel->rd_node.dbNode, rel->rd_node.relNode, col, heapBlk)));

    needwal = (RelationNeedsWAL(rel) && !t_thrd.xlog_cxt.InRecovery);

    if (status != bcmStatus) {
        START_CRIT_SECTION();

        /* set status */
        SET_SYNC_BYTE_STATUS(map[mapByte], status, bshift);
        MarkBufferDirty(buf);

        /*
         * we record one cu bcm xlog in BCMLogCU for column store.
         */
        bool isRowStore = (col == 0);

        if (needwal && isRowStore) {
            XLogRecPtr recptr = InvalidXLogRecPtr;

            recptr = log_heap_bcm(&(rel->rd_node), 0, heapBlk, status);

            PageSetLSN(page, recptr);
        }
        END_CRIT_SECTION();
    }
}

/* Clear all the bcm bits of a relation */
void BCMClearRel(Relation rel, int col)
{
    BlockNumber totalblocks = 0;
    BlockNumber mapBlock;
    ForkNumber forknum = BCM_FORKNUM;

#ifdef TRACE_BCMMAP
    elog(LOG, "BCMClearRel %s", RelationGetRelationName(rel));
#endif

    if (col > 0) {
        forknum = ColumnId2ColForkNum(col);
        CStoreRelationOpenSmgr(rel, col);
    } else {
        RelationOpenSmgr(rel);
    }

    /*
     * If no bcm map has been created yet for this relation, there's
     * nothing to clear.
     */
    if (!smgrexists(rel->rd_smgr, forknum))
        return;

    totalblocks = smgrnblocks(rel->rd_smgr, forknum);
    /*
     * If  bcm map only has a file header, there's nothing to clear.
     */
    if (totalblocks == 0 || totalblocks == 1)
        return;

    /* We begin clear from page 1 not page 0 */
    for (mapBlock = 1; mapBlock < totalblocks; mapBlock++) {
        Buffer mapBuffer;
        unsigned char *map = NULL;
        errno_t rc = 0;

        mapBuffer = BCM_readbuf(rel, mapBlock, false, col);
        if (!BufferIsValid(mapBuffer))
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED), errmsg("%u/%u/%u invalid bcm buffer %u", rel->rd_node.spcNode,
                                                             rel->rd_node.dbNode, rel->rd_node.relNode, mapBlock)));

        LockBuffer(mapBuffer, BUFFER_LOCK_EXCLUSIVE);
        map = (unsigned char *)PageGetContents(BufferGetPage(mapBuffer));

        /* NB: We clear the whole page, including the dcm bits, is that ok? */
        rc = memset_s(map, BCMMAPSIZE, 0, BCMMAPSIZE);
        securec_check(rc, "", "");
        MarkBufferDirty(mapBuffer);

        UnlockReleaseBuffer(mapBuffer);
    }
}

/*
 * BCM_truncate - truncate the bcm map
 *
 * The caller must hold AccessExclusiveLock on the relation, to ensure that
 * other backends receive the smgr invalidation event that this function sends
 * before they access the bcm again.
 *
 * Note: bcm will be truncated to zero. Only data replication can generate bcm
 * file, and heap can not be truncated by lazy vacuum(the function of
 * lazy_truncate_heap has been disabled at data replication mode), so we need
 * not to realize the code about truncate the bcm file to nblock.
 */
void BCM_truncate(Relation rel)
{
#ifdef TRACE_BCMMAP
    ereport(DEBUG1, (errmodule(MOD_REP), errmsg("bcm_truncate %s", RelationGetRelationName(rel))));
#endif

    RelationOpenSmgr(rel);

    /*
     * If no bcm map has been created yet for this relation, there's
     * nothing to truncate.
     */
    if (!smgrexists(rel->rd_smgr, BCM_FORKNUM))
        return;

    /* Truncate the bcm pages, and send smgr inval message */
    smgrtruncate(rel->rd_smgr, BCM_FORKNUM, 0);

    /*
     * We might as well update the local smgr_bcm_nblocks setting. smgrtruncate
     * sent an smgr cache inval message, which will cause other backends to
     * invalidate their copy of smgr_bcm_nblocks, and this one too at the next
     * command boundary.  But this ensures it isn't outright wrong until then.
     */
    for (int i = 0; i < rel->rd_smgr->smgr_bcmarry_size; i++)
        rel->rd_smgr->smgr_bcm_nblocks[i] = 0;
}

/*
 * Read a bcm map page.
 *
 * If the page doesn't exist, InvalidBuffer is returned, or if 'extend' is
 * true, the bcm map file is extended.
 */
static Buffer BCM_readbuf(Relation rel, BlockNumber blkno, bool extend, int col)
{
    Buffer buf;
    ForkNumber forknum = BCM_FORKNUM;

    if (col > 0) {
        forknum = ColumnId2ColForkNum(col);

        /*
         * We might not have opened the relation at the smgr level yet, or we
         * might have been forced to close it by a sinval message.	The code below
         * won't necessarily notice relation extension immediately when extend =
         * false, so we rely on sinval messages to ensure that our ideas about the
         * size of the map aren't too far out of date.
         */
        CStoreRelationOpenSmgr(rel, col);
    } else {
        RelationOpenSmgr(rel);
    }

    /*
     * If we haven't cached the size of the bcm map fork yet, check it
     * first.
     */
    if (rel->rd_smgr->smgr_bcm_nblocks[col] == InvalidBlockNumber) {
        if (smgrexists(rel->rd_smgr, forknum))
            rel->rd_smgr->smgr_bcm_nblocks[col] = smgrnblocks(rel->rd_smgr, forknum);
        else
            rel->rd_smgr->smgr_bcm_nblocks[col] = 0;
    }

    /* Handle requests beyond EOF */
    if (blkno >= rel->rd_smgr->smgr_bcm_nblocks[col]) {
        if (extend)
            BCM_extend(rel, blkno + 1, col);
        else
            return InvalidBuffer;
    }

    /*
     * Use ZERO_ON_ERROR mode, and initialize the page if necessary. It's
     * always safe to clear bits, so it's better to clear corrupt pages than
     * error out.
     */
    buf = ReadBufferExtended(rel, forknum, blkno, RBM_ZERO_ON_ERROR, NULL);
    if (PageIsNew(BufferGetPage(buf)))
        PageInit(BufferGetPage(buf), BLCKSZ, 0);
    return buf;
}

/*
 * Ensure that the bcm map fork is at least bcm_nblocks long, extending
 * it if necessary with zeroed pages.
 */
static void BCM_extend(Relation rel, BlockNumber bcm_nblocks, int col)
{
    BlockNumber bcm_nblocks_now;
    Page pg;
    ForkNumber forknum = BCM_FORKNUM;

    ADIO_RUN()
    {
        pg = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        pg = (Page)palloc(BLCKSZ);
    }
    ADIO_END();

    PageInit(pg, BLCKSZ, 0);

    /*
     * We use the relation extension lock to lock out other backends trying to
     * extend the bcm map at the same time. It also locks out extension
     * of the main fork, unnecessarily, but extending the bcm map
     * happens seldom enough that it doesn't seem worthwhile to have a
     * separate lock tag type for it.
     *
     * Note that another backend might have extended or created the relation
     * by the time we get the lock.
     */
    LockRelationForExtension(rel, ExclusiveLock);

    /*
     * Create the file first if it doesn't exist.  If smgr_bcm_nblocks is
     * positive then it must exist, no need for an smgrexists call.
     */
    if (col > 0) {
        forknum = ColumnId2ColForkNum(col);
        CStoreRelationOpenSmgr(rel, col);
    } else {
        /* Might have to re-open if a cache flush happened */
        RelationOpenSmgr(rel);
    }

    if ((rel->rd_smgr->smgr_bcm_nblocks[col] == 0 || rel->rd_smgr->smgr_bcm_nblocks[col] == InvalidBlockNumber) &&
        !smgrexists(rel->rd_smgr, forknum)) {
        createBCMFile(rel, col);
    }

    bcm_nblocks_now = smgrnblocks(rel->rd_smgr, forknum);

    if (bcm_nblocks_now < bcm_nblocks) {
        VerifyTblspcWhenBcmExtend(rel, col, bcm_nblocks - bcm_nblocks_now);
    }

    /* Now extend the file */
    while (bcm_nblocks_now < bcm_nblocks) {
        PageSetChecksumInplace(pg, bcm_nblocks_now);

        smgrextend(rel->rd_smgr, forknum, bcm_nblocks_now, (char *)pg, false);
        bcm_nblocks_now++;
    }

    /*
     * Send a shared-inval message to force other backends to close any smgr
     * references they may have for this rel, which we are about to change.
     * This is a useful optimization because it means that backends don't have
     * to keep checking for creation or extension of the file, which happens
     * infrequently.
     */
    CacheInvalidateSmgr(rel->rd_smgr->smgr_rnode);

    /* Update local cache with the up-to-date size */
    rel->rd_smgr->smgr_bcm_nblocks[col] = bcm_nblocks_now;

    UnlockRelationForExtension(rel, ExclusiveLock);

    ADIO_RUN();
    {
        adio_align_free(pg);
    }
    ADIO_ELSE()
    {
        pfree(pg);
        pg = NULL;
    }
    ADIO_END();
}

/* Read bcm page */
void BCM_CStore_pin(Relation rel, int col, uint64 offset, Buffer *buf)
{
    Assert(col > 0);
    uint64 align_size = (uint64)(uint32)CUAlignUtils::GetCuAlignSizeColumnId(col);
    BlockNumber mapBlock = cstore_offset_to_bcmblock(offset, align_size);
    *buf = BCM_readbuf(rel, mapBlock, true, col);
}

/* Read bcm page */
void BCM_pin(Relation rel, BlockNumber heapBlk, Buffer *buf)
{
    BlockNumber mapBlock = HEAPBLK_TO_BCMBLOCK(heapBlk);

    *buf = BCM_readbuf(rel, mapBlock, true);
}

/*
 * BCMSendData
 *
 * Traverse every BCM page of current relation to see if a corresponding
 * heap page or CU unit needs to send to standby. If needed, load the
 * heap page or CU unit data and push it to the send queue. We should
 * hold the relation lock to avoid been dropped during catchup.
 * In order to speed up the check efficiency, we just need to walk the
 * bcm meta buffer instead. More comments see in bcm meta buffer.
 */
static void BCMSendData(const RelFileNode &relfilenode, const char *bcmpath, int col)
{
    RelFileNode InvalidRelFileNode = { 0, 0, 0, -1, 0};
    Relation rel;
    Buffer metabuffer = InvalidBuffer;
    ForkNumber forknum = BCM_FORKNUM;
    BlockNumber heapBlock = InvalidBlockNumber;
    BlockNumber metanum = 1;
    BlockNumber maxHeapBlock = InvalidBlockNumber;
    struct stat stat_buf;

    volatile DataSndCtlData *datasndctl = t_thrd.datasender_cxt.DataSndCtl;
    bool isColStore = col > 0 ? true : false;
    int contibits = 0;

    /* if disabled stream replication or relfilenode is invalid, skip current relation. */
    if (!u_sess->attr.attr_storage.enable_stream_replication ||
        (0 == memcmp(&relfilenode, &InvalidRelFileNode, sizeof(RelFileNode))))
        return;

    /*
     * Here we lock the database to solve the checkpoint failure " ERROR:checkpoint request failed
     * CONTEXT:  Error message received from nodes:xxx" because of the concurrent execution of drop
     * database and catchup.Steps to reproduce:
     * 1.create database test,and create table t1 in test;
     * 2.copy data to t1(without standby)
     * 3.drop database and sleep before rm data
     * 4.start standby, catchup thread will start and send data in primary
     * 5.conitue step 3
     * 6.drop database will success
     * 7.create database or checkpoint will get the error.
     */
    LockSharedObject(DatabaseRelationId, relfilenode.dbNode, 0, RowExclusiveLock);

    rel = CreateFakeRelcacheEntry(relfilenode);

    /*
     * First lock relfilenode(Notes: relfilenode.relNode maybe differnt from oid),
     * at this time, LockRelation is equal to LockRelFileNode,
     * then read the bcm file, if it is not exit,
     * the table maybe delete, so we will return.
     *
     * ExclusiveLock will block insert, because catchup maybe read a zero block
     * after insert, it is tested on xfs file system.
     */
    LockRelFileNode(relfilenode, ExclusiveLock);

    if (isColStore) {
        forknum = ColumnId2ColForkNum(col);
        CStoreRelationOpenSmgr(rel, col);
    } else {
        RelationOpenSmgr(rel);
    }

    /*
     * BCM file is just removed, skip following check.
     * smgrexists maybe not correct(After the table is dropped), so we should use
     * stat to check it.
     */
    if (!smgrexists(rel->rd_smgr, forknum) || stat(bcmpath, &stat_buf) != 0) {
        UnlockRelFileNode(relfilenode, ExclusiveLock);
        FreeFakeRelcacheEntry(rel);
        UnlockSharedObject(DatabaseRelationId, relfilenode.dbNode, 0, RowExclusiveLock);
        return;
    }

    BCMClearMetaBit(rel, col);

    metabuffer = BCM_readbuf(rel, metanum, false, col);
    if (!BufferIsValid(metabuffer)) {
        /* Nothing to do, the file was already smaller */
        UnlockRelFileNode(relfilenode, ExclusiveLock);
        FreeFakeRelcacheEntry(rel);
        UnlockSharedObject(DatabaseRelationId, relfilenode.dbNode, 0, RowExclusiveLock);
        return;
    }

    /* get max size of data file */
    maxHeapBlock = BCMGetDataFileMaxSize(rel, col);
    if (maxHeapBlock == InvalidBlockNumber) {
        /* Nothing to do, the file size was zero */
        UnlockRelFileNode(relfilenode, ExclusiveLock);
        FreeFakeRelcacheEntry(rel);
        UnlockSharedObject(DatabaseRelationId, relfilenode.dbNode, 0, RowExclusiveLock);
        return;
    }

    CUFile *cFile = isColStore ? New(CurrentMemoryContext) CUFile(relfilenode, col) : NULL;
    do {
        ereport(DEBUG3, (errmsg("valid bcm meta buffer :%u", metanum)));

        BCMWalkMetaBuffer(rel, cFile, metabuffer, heapBlock, contibits, maxHeapBlock, col);
        ReleaseBuffer(metabuffer);

        /* caculate the next meta page, than check again. */
        metanum += META_BLOCKS_PER_PAGE + 1;
        metabuffer = BCM_readbuf(rel, metanum, false, col);
    } while (BufferIsValid(metabuffer));

    /*
     * For column store, after we loaded all the bcm buffers, especially when
     * the last bcm status was NOTSYNCED, we should finish the surplus work --
     * push the last contibits data to queue.
     */
    if (contibits > 0)
        bcm_read_multi_cu(cFile, rel, col, heapBlock, contibits, maxHeapBlock);

    if (cFile)
        DELETE_EX(cFile);

    /*
     * we should wait until all the pushed data has been send to the standby,
     * then clear the BCMArray.
     */
    while (DQByteLT(datasndctl->queue_offset, t_thrd.proc->waitDataSyncPoint)) {
        CatchupShutdownIfNoDataSender();
        pg_usleep(1000L); /* 1ms */
    }

    ClearBCMArray();
    BCMResetMetaBit(rel, metanum, col);

    UnlockRelFileNode(relfilenode, ExclusiveLock);
    FreeFakeRelcacheEntry(rel);

    UnlockSharedObject(DatabaseRelationId, relfilenode.dbNode, 0, RowExclusiveLock);
}

/*
 * BCMWalkMetaBuffer
 *
 * Walk through every bit in current meta page to find out if any corresponding
 * BCM page needs to search.
 */
static void BCMWalkMetaBuffer(Relation rel, CUFile *cFile, Buffer metabuffer, BlockNumber &heapBlock, int &contibits,
                              BlockNumber maxHeapBlock, int col)
{
    Buffer bcmbuffer = InvalidBuffer;
    BlockNumber metaBlock;
    BlockNumber bcmBlock;
    int i;
    int j;
    uint32 bshift;
    BCMBitStatus status;
    Page metapage;
    unsigned char *map = NULL;

    Assert(BufferIsValid(metabuffer));
    metaBlock = BufferGetBlockNumber(metabuffer);
    metapage = BufferGetPage(metabuffer);
    map = (unsigned char *)PageGetContents(metapage);

    for (i = 0; i < (int)BCMMAPSIZE; i++) {
        for (j = 0; j < META_BLOCKS_PER_BYTE; j++) {
            bshift = (uint32)j * META_BITS_PER_BLOCK;
            status = ((map[i] >> bshift) & META_SYNC1_BITMASK) >> 1;

            /* the bcm block needs to sync */
            if (status == NOTSYNCED) {
                CatchupShutdownIfNoDataSender();
                /* get bcm page block */
                bcmBlock = GET_BCM_BLOCK(metaBlock, i, j);
                ereport(DEBUG2, (errmsg("relation %u/%u/%u col %d try to sync bcm block %u", rel->rd_node.spcNode,
                                        rel->rd_node.dbNode, rel->rd_node.relNode, col, bcmBlock)));
                /*
                 * We assume that if the bcm buffer is invalid, it means that some
                 * thread has just extended that block, and we can see it in meta page
                 * but not in the opened smgr of current relation. It's safe to skip this
                 * block 'cause we can sync it by data replication.
                 */
                bcmbuffer = BCM_readbuf(rel, bcmBlock, false, col);
                if (BufferIsValid(bcmbuffer)) {
                    BCMSendOneBuffer(rel, cFile, bcmbuffer, heapBlock, contibits, maxHeapBlock, col);
                    ReleaseBuffer(bcmbuffer);
                }
            }
        }
    }
}

/*
 * BCMSendOneBuffer
 *
 * Walk through every bit in current bcm page to find out if any corresponding
 * heap pages or CU units need to send to standby.
 */
static void BCMSendOneBuffer(Relation rel, CUFile *cFile, Buffer bcmbuffer, BlockNumber &heapBlock, int &contibits,
                             BlockNumber maxHeapBlock, int col)
{
    Buffer heapbuffer = InvalidBuffer;
    Page bcmpage;
    int i;
    int j;
    uint32 bshift;
    unsigned char *map = NULL;
    BCMBitStatus status;
    BlockNumber blocknum = 0;
    bool isColStore = col > 0 ? true : false;

    blocknum = BufferGetBlockNumber(bcmbuffer);
    Assert(isColStore || (cFile == NULL));

    /*
     * Do not lock buffer, maybe deadlock, if
     * Catchup held this buffer share lock, and push to dataqueue, but queue has no freespace,
     * Catchup will sleep with share lock; wait for DataSender to free queue's space;
     * But DataSender need to get this buffer exclusive lock to set BCM bit: 1-->0, so
     * Catchup held share lock wait DataSender; DataSender wait exclusive lock held by Catchup;
     * then deadlock occured.
     */
    bcmpage = BufferGetPage(bcmbuffer);
    map = (unsigned char *)PageGetContents(bcmpage);

    for (i = 0; i < (int)BCMMAPSIZE; i++) {
        for (j = 0; j < BCM_BLOCKS_PER_BYTE; j++) {
            bshift = (uint32)j * BCM_BITS_PER_BLOCK;
            status = ((map[i] >> bshift) & BCM_SYNC_BITMASK) >> 1;

            /* If not sync */
            if (status == NOTSYNCED) {
                CatchupShutdownIfNoDataSender();
                if (isColStore) { /* column store */
                    /* get heap page block */
                    if (contibits == 0)
                        heapBlock = GET_HEAP_BLOCK(blocknum, i, j);
                    contibits++;
                } else { /* row store */
                    /* get heap page block */
                    heapBlock = GET_HEAP_BLOCK(blocknum, i, j);

                    if (u_sess->attr.attr_storage.HaModuleDebug) {
                        ereport(LOG, (errmsg("HA-BCMSendOneBuffer: relation %u/%u/%u col %d try to sync bcm "
                                             "blockno %u heap blockno %u maxHeapBlock %u",
                                             rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, col,
                                             blocknum, heapBlock, maxHeapBlock)));
                    }

                    /*
                     * For OS crash, Data file block maybe not fsync disk to 100(For example),
                     * but BCM maybe flush disk to 101. We can not read data file block.
                     */
                    if (heapBlock > maxHeapBlock)
                        return;

                    heapbuffer = ReadBuffer(rel, heapBlock);

                    LockBuffer(heapbuffer, BUFFER_LOCK_SHARE);
                    PushHeapPageToDataQueue(heapbuffer);
                    UnlockReleaseBuffer(heapbuffer);
                }
            }

            if (isColStore) {
                /*
                 * for column store, we record the continuous no-sync status,
                 * load CU data for once as much as possible.
                 */
                int max_contibits = (512 * 1024) / CUAlignUtils::GetCuAlignSizeColumnId(col);
                if (contibits > 0 && (status == SYNCED || contibits >= max_contibits))
                    bcm_read_multi_cu(cFile, rel, col, heapBlock, contibits, maxHeapBlock);
            }
        }
    }
}

/*
 * Get max block num for bcm file relfilenode
 */
static BlockNumber BCMGetDataFileMaxSize(Relation rel, int col)
{
    BlockNumber maxHeapBlock = 0;

    if (col > 0) {
        uint64 filesize = GetColDataFileSize(rel, col);
        maxHeapBlock = (BlockNumber)(filesize / CUAlignUtils::GetCuAlignSizeColumnId(col));
    } else {
        if (smgrexists(rel->rd_smgr, MAIN_FORKNUM)) {
            maxHeapBlock = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
        } else {
            char *rpath = NULL;
            RelFileNodeBackend smgr_rnode;
            smgr_rnode.node = rel->rd_node;
            smgr_rnode.backend = InvalidBackendId;
            rpath = relpath(smgr_rnode, MAIN_FORKNUM);
            ereport(WARNING, (errcode_for_file_access(), errmsg("relation file is not exist when get max block num "
                                                                "for bcm file relfilenode: \"%s\": %m",
                                                                rpath)));
            pfree(rpath);
            rpath = NULL;
        }
    }

    /*
     * Block is from 0 to nblocks-1, if maxHeapBlock is 0, we should return 0
     */
    return maxHeapBlock ? (maxHeapBlock - 1) : InvalidBlockNumber;
}

/*
 * Check if we have specific postfix in the string.
 */
static bool CheckFilePostfix(const char *str1, const char *str2)
{
    int len1 = 0;
    int len2 = 0;
    if (str1 == NULL || str2 == NULL) {
        return false;
    }
    len1 = (int)strlen(str1);
    len2 = (int)strlen(str2);
    if ((len1 < len2) || (len1 == 0 || len2 == 0)) {
        return false;
    }
    while (len2 >= 1) {
        if (str2[len2 - 1] != str1[len1 - 1]) {
            return false;
        }
        len2--;
        len1--;
    }
    return true;
}

/*
 * BCMClearFile: set the BCM file's pages to init pages
 * except the first page(BCM File Header).
 * FUTURE CASE:: Maybe we should Consider concurrency scenarios,
 * one is clearing file another is setting.
 */
static void BCMClearFile(const RelFileNode &relfilenode, int col)
{
    RelFileNode InvalidRelFileNode = { 0, 0, 0, -1, 0};
    Relation rel;

    if (0 == memcmp(&relfilenode, &InvalidRelFileNode, sizeof(RelFileNode)))
        return;

    rel = CreateFakeRelcacheEntry(relfilenode);
    BCMClearRel(rel, col);
    FreeFakeRelcacheEntry(rel);
}

/* Recursion search BCM files with the tableSpacePath */
static void searchBCMFiles(const char *tableSpacePath, const char *relativepath, bool undertablespace, bool clear,
                           int iterations)
{
    DIR *dir = NULL;
    struct dirent *de;
    char path[MAXPGPATH] = {'\0'};
    char rpath[MAXPGPATH] = {'\0'};
    int nRet = 0;

    /* the layer number of searchBCMFiles iterations */
    iterations++;

    dir = AllocateDir(tableSpacePath);
    while ((de = ReadDir(dir, tableSpacePath)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        if (strncmp(de->d_name, PG_TEMP_FILE_PREFIX, strlen(PG_TEMP_FILE_PREFIX)) == 0)
            continue;

        if (strncmp(de->d_name, "pg_log", strlen("pg_log")) == 0 ||
            strncmp(de->d_name, "pg_location", strlen("pg_location")) == 0)
            continue;

        if (strncmp(de->d_name, "pg_xlog", strlen("pg_xlog")) == 0)
            continue;

        if (strncmp(de->d_name, "full_upgrade_bak", strlen("full_upgrade_bak")) == 0)
            continue;

        nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s", tableSpacePath, de->d_name);
        securec_check_ss(nRet, "", "");

        if (undertablespace) {
            if (NULL == strstr(path, TABLESPACE_VERSION_DIRECTORY) ||
                NULL == strstr(path, g_instance.attr.attr_common.PGXCNodeName))
                continue;
        } else {
            if (strcmp(de->d_name, "pg_tblspc") == 0)
                continue;
        }

        if (relativepath) {
            nRet = snprintf_s(rpath, sizeof(rpath), MAXPGPATH - 1, "%s/%s", relativepath, de->d_name);
            securec_check_ss(nRet, "", "");
        } else {
            nRet = snprintf_s(rpath, sizeof(rpath), MAXPGPATH - 1, "%s", de->d_name);
            securec_check_ss(nRet, "", "");
        }

        /*
         * serchBCMFiles will be recursive call 3 interations to get file path. In the third layer,
         * the file path is table file, not Dir, so we need not to decide whether it is a folder,
         * because the performance of stat interface is too bad. The file path such as
         * ./base/13764 or /home/xxx/tablespace/PG_9.2_201611171_datanode1/13764
         */
        if (iterations < 3 && isDirExist(path)) {
            ereport(DEBUG3, (errmsg("search path %s, relative path: %s, iterations: %d.", path, rpath, iterations)));
            searchBCMFiles(path, rpath, undertablespace, clear, iterations);
        } else {
            /*
             * When we handle the bcm files, we will find if we end with "_bcm".
             */
            if (CheckFilePostfix(rpath, BCM)) {
                HandleBCMfile(rpath, clear);
            }
        }
    }
    FreeDir(dir);
}

static void HandleBCMfile(char *bcmpath, bool clear)
{
    RelFileNodeForkNum bcmfilenode;

    bcmfilenode = relpath_to_filenode(bcmpath);
    if (bcmfilenode.forknumber == InvalidForkNumber) {
        ereport(WARNING,
                (errmsg("relfilenode [spcNode%u] [dbNode%u] [relNode%u]"
                        "[backendId%d] [segno%u] [forkNumber-%d] forkNumber is invalid",
                        bcmfilenode.rnode.node.spcNode, bcmfilenode.rnode.node.dbNode, bcmfilenode.rnode.node.relNode,
                        bcmfilenode.rnode.backend, bcmfilenode.segno, bcmfilenode.forknumber)));
        return;
    }

    ereport(DEBUG3,
            (errmsg("relfilenode [spcNode%u] [dbNode%u] [relNode%u]"
                    "[backendId%d] [segno%u] [forkNumber-%d]",
                    bcmfilenode.rnode.node.spcNode, bcmfilenode.rnode.node.dbNode, bcmfilenode.rnode.node.relNode,
                    bcmfilenode.rnode.backend, bcmfilenode.segno, bcmfilenode.forknumber)));

    if (clear) {
        /* Clear this bcm file */
        ereport(DEBUG2, (errmsg("clear bcm file %s ", bcmpath)));
        BCMClearFile(bcmfilenode.rnode.node, GetColumnNum(bcmfilenode.forknumber));
    } else {
        /*
         * According to bcm file bcmPath, we put the data(not synchronized)
         * to the queue.
         */
        ereport(DEBUG2, (errmsg("according to bcm file %s, send data(not synchronized)", bcmpath)));

        CatchupShutdownIfNoDataSender();
        BCMSendData(bcmfilenode.rnode.node, bcmpath, GetColumnNum(bcmfilenode.forknumber));
    }
}

/* Get all bcm files, clear all or send the according not sync heap blocks */
void GetBcmFileList(bool clear)
{
    DIR *dir = NULL;
    List *tablespaces = NIL;
    ListCell *lc = NULL;
    struct dirent *de;
    tablespaceinfo *ti = NULL;

    MemoryContext bcm_context;
    MemoryContext old_context;

    int nRet = 0;

    bcm_context = AllocSetContextCreate(CurrentMemoryContext, "Search BCM files context", ALLOCSET_DEFAULT_MINSIZE,
                                        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    old_context = MemoryContextSwitchTo(bcm_context);
    ereport(LOG, (errmsg("catchup process start to search all of bcm files.")));

    /* Make sure we can open the directory with tablespaces in it. */
    dir = AllocateDir("pg_tblspc");
    if (!dir) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", "pg_tblspc")));
        return;
    }

    /* Collect information about all tablespaces. */
    while ((de = ReadDir(dir, "pg_tblspc")) != NULL) {
        char fullpath[MAXPGPATH];
        char linkpath[MAXPGPATH];
        int rllen;

        /* Skip special stuff */
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        nRet = snprintf_s(fullpath, sizeof(fullpath), sizeof(fullpath) - 1, "pg_tblspc/%s", de->d_name);
        securec_check_ss(nRet, "", "");

#if defined(HAVE_READLINK) || defined(WIN32)
        rllen = readlink(fullpath, linkpath, sizeof(linkpath));
        if (rllen < 0) {
            ereport(WARNING, (errmsg("could not read symbolic link \"%s\": %m", fullpath)));
            continue;
        } else if (rllen >= (int)sizeof(linkpath)) {
            ereport(WARNING, (errmsg("symbolic link \"%s\" target is too long", fullpath)));
            continue;
        }
        linkpath[rllen] = '\0';

        ti = (tablespaceinfo *)palloc(sizeof(tablespaceinfo));
        ti->oid = pstrdup(de->d_name);
        ti->path = pstrdup(linkpath);
        ti->relativePath = pstrdup(fullpath);
        ti->size = -1;
        tablespaces = lappend(tablespaces, ti);
#else

        /*
         * If the platform does not have symbolic links, it should not be
         * possible to have tablespaces - clearly somebody else created
         * them. Warn about it and ignore.
         */
        ereport(WARNING,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tablespaces are not supported on this platform")));
#endif
    }

    /* Add a node for the base directory at the end */
    ti = (tablespaceinfo *)palloc0(sizeof(tablespaceinfo));
    tablespaces = lcons(ti, tablespaces);

    foreach (lc, tablespaces) {
        tablespaceinfo *tsi = (tablespaceinfo *)lfirst(lc);
        if (tsi->path != NULL) {
            /* Tablespace create by user */
            ereport(DEBUG1, (errmsg("bcm path: %s; relative path: %s.", tsi->path, tsi->relativePath)));
            searchBCMFiles(tsi->path, tsi->relativePath, true, clear, 0);
        } else {
            /* Default tablespace */
            ereport(DEBUG1, (errmsg("bcm path: %s; relative path: %s.", ".", ".")));
            searchBCMFiles(".", NULL, false, clear, 0);
        }
    }

    FreeDir(dir);
    ereport(LOG, (errmsg("catchup process done to search all bcm files.")));

    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(bcm_context);
}

/* Get incremental bcm files, clear all or send the according not sync heap blocks */
void GetIncrementalBcmFileList()
{
    int num = 0;
    char path[MAXPGPATH] = {'\0'};
    char *temp = NULL;
    char *fileList = NULL;
    int msgLength = 0;
    errno_t errorno = EOK;
    ereport(LOG, (errmsg("catchup process start to search incremental bcm files.")));
    int getIncrementalCatchupParseBcmTime = 0;
    int getIncrementalCatchupHandleBcmTime = 0;
    num = g_incrementalBcmInfo.msgLength / sizeof(RelFileNodeKey);
    msgLength = g_incrementalBcmInfo.msgLength;
    fileList = g_incrementalBcmInfo.receivedFileList;

    temp = fileList;
    ereport(LOG, (errmsg("num of file list we got from dummy:%d", num)));

    while (num != 0) {
        TimestampTz parseBcmStartTime = GetCurrentTimestamp();
        RelFileNodeKey data;
        errorno = memcpy_s((void *)&data, sizeof(RelFileNodeKey), temp, sizeof(RelFileNodeKey));
        securec_check(errorno, "", "");
        temp += sizeof(RelFileNodeKey);

        if ((int)data.relfilenode.spcNode == DEFAULTTABLESPACE_OID) {
            GetIncrementalBcmFilePathForDefault(data, path, sizeof(path));
        } else {
            GetIncrementalBcmFilePathForCustome(data, path, sizeof(path));
        }

        getIncrementalCatchupParseBcmTime += ComputeTimeStamp(parseBcmStartTime);
        if (*path != '\0') {
            TimestampTz handleBcmStartTime = GetCurrentTimestamp();
            HandleBCMfile(path, false);
            getIncrementalCatchupHandleBcmTime += ComputeTimeStamp(handleBcmStartTime);
        }
        num--;
    }
    ReplaceOrFreeBcmFileListBuffer(NULL, 0);
    ereport(
        LOG,
        (errmsg("incremental catchup parsing bcm costs %d milliseconds, handling bcm costs %d milliseconds, and total "
                "costs %d milliseconds",
                getIncrementalCatchupParseBcmTime, getIncrementalCatchupHandleBcmTime,
                getIncrementalCatchupParseBcmTime + getIncrementalCatchupHandleBcmTime)));
    ereport(LOG, (errmsg("catchup process done to search incremental bcm files.")));
}

/* Get incremental bcm file path for default tablespace path example: base/dbnode/relnode_BCM */
static void GetIncrementalBcmFilePathForDefault(const RelFileNodeKey &data, char *path, int length)
{
    int nRet = 0;

    if ((int)data.relfilenode.spcNode == DEFAULTTABLESPACE_OID) {
        if (data.columnid != 0) {
            nRet = snprintf_s(path, length, length - 1, "base/%u/%u_C%d_bcm", data.relfilenode.dbNode,
                              data.relfilenode.relNode, data.columnid);
            securec_check_ss(nRet, "", "");
        } else {
            nRet = snprintf_s(path, length, length - 1, "base/%u/%u_bcm", data.relfilenode.dbNode,
                              data.relfilenode.relNode);
            securec_check_ss(nRet, "", "");
        }
        if (u_sess->attr.attr_storage.HaModuleDebug) {
            ereport(LOG, (errmsg("default tablespace path :%s\n", path)));
        }
    }
}

/* Get incremental bcm file path for custome tablespace path example:
 * pg_tblspc/spcnode/version_nodename/dbnode/relnode_BCM */
static void GetIncrementalBcmFilePathForCustome(const RelFileNodeKey &data, char *path, int length)
{
    int nRet = 0;
    DIR *dir = NULL;
    char fullPath[MAXPGPATH];
    char linkPath[MAXPGPATH];
    int readLinkPathLength;

    if ((int)data.relfilenode.spcNode != DEFAULTTABLESPACE_OID) {
        /* Check pg_tblspc dir */
        dir = AllocateDir("pg_tblspc");
        if (!dir) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", "pg_tblspc")));
            return;
        }
        FreeDir(dir);

        /* pg_tblspc/spcnode */
        nRet = snprintf_s(fullPath, sizeof(fullPath), sizeof(fullPath) - 1, "pg_tblspc/%u", data.relfilenode.spcNode);
        securec_check_ss(nRet, "", "");

#if defined(HAVE_READLINK) || defined(WIN32)
        /* Check link path */
        readLinkPathLength = readlink(fullPath, linkPath, sizeof(linkPath));
        if (readLinkPathLength < 0) {
            ereport(WARNING, (errmsg("could not read symbolic link \"%s\": %m", fullPath)));
            return;
        } else if (readLinkPathLength >= (int)sizeof(linkPath)) {
            ereport(WARNING, (errmsg("symbolic link \"%s\" target is too long", fullPath)));
            return;
        }
        linkPath[readLinkPathLength] = '\0';
#else
        /*
         * If the platform does not have symbolic links, it should not be
         * possible to have tablespaces - clearly somebody else have created
         * them. Warn about it and ignore.
         */
        ereport(WARNING,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tablespaces are not supported on this platform")));
#endif

        if (data.columnid != 0) {
            /* pg_tblspc/spcnode/version_nodename/dbnode/relnode_C1_BCM */
            nRet = snprintf_s(path, length, length - 1, "%s/%s_%s/%u/%u_C%d_bcm", fullPath,
                              TABLESPACE_VERSION_DIRECTORY, g_instance.attr.attr_common.PGXCNodeName,
                              data.relfilenode.dbNode, data.relfilenode.relNode, data.columnid);
            securec_check_ss(nRet, "", "");
        } else {
            /* pg_tblspc/spcnode/version_nodename/dbnode/relnode_BCM */
            nRet = snprintf_s(path, length, length - 1, "%s/%s_%s/%u/%u_bcm", fullPath, TABLESPACE_VERSION_DIRECTORY,
                              g_instance.attr.attr_common.PGXCNodeName, data.relfilenode.dbNode,
                              data.relfilenode.relNode);
            securec_check_ss(nRet, "", "");
        }
        if (u_sess->attr.attr_storage.HaModuleDebug) {
            ereport(LOG, (errmsg("custome tablespace BCM path:%s\n", path)));
        }
    }
}

/*
 * Load multiple CU units to buffer, push data to sender queue.
 * Cause the CU manager may not return the exact size we expected,
 * so try again until we get the data we need.
 */
static void bcm_read_multi_cu(CUFile *cFile, Relation rel, int col, BlockNumber heapBlock, int &contibits,
                              BlockNumber maxHeapBlock)
{
    uint64 align_size = (uint64)(uint32)CUAlignUtils::GetCuAlignSizeColumnId(col);
    uint64 offset = align_size * (uint64)heapBlock;
    char *write_buf = NULL;
    int realSize = 0;

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG, (errmsg("HA-bcm_read_multi_cu: relation %u/%u/%u col %d try to sync "
                             "cu blockno %u, contibits %d, maxHeapBlock %u",
                             rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, col, heapBlock, contibits,
                             maxHeapBlock)));
    }

    /* The heapBlock of data file must be not exist */
    if (heapBlock > maxHeapBlock) {
        contibits = 0;
        return;
    }

    /* we should send the NOTSYNCED data from heapBlock to maxHeapBlock */
    contibits = (int)Min((uint32)contibits, maxHeapBlock - heapBlock + 1);

    while (contibits > 0) {
        CatchupShutdownIfNoDataSender();
        write_buf = cFile->Read(offset, align_size * contibits, &realSize, (int)align_size);
        if (write_buf == NULL) {
            Assert(realSize == 0);
            contibits = 0;
            return;
        }

        if (u_sess->attr.attr_storage.HaModuleDebug)
            check_cu_block(write_buf, realSize, (int)align_size);

        PushCUToDataQueue(rel, col, write_buf, offset, realSize, false);
        ereport(DEBUG3, (errmsg("cuBlock %u col %d read and send data's realsize is %d.", heapBlock, col, realSize)));
        offset += realSize;
        contibits -= realSize / align_size;
    }
    Assert(contibits == 0);
}

void check_cu_block(char *mem, int size, int alignSize)
{
    Assert(alignSize > 0);
    int cuUnit = size / alignSize;
    char zeroBlock[alignSize] = {0};
    char *mem_temp = mem;

    for (int i = 0; i < cuUnit; i++) {
        if (memcmp(mem_temp, zeroBlock, alignSize) == 0)
            ereport(WARNING, (errmsg("HA-check_cu_block: check cu blockno %d failed, it is zeropage", i)));

        mem_temp += alignSize;
    }
}

uint64 cstore_offset_to_cstoreblock(uint64 offset, uint64 align_size)
{
    return offset / align_size;
}

uint64 cstore_offset_to_bcmblock(uint64 offset, uint64 align_size)
{
    uint64 cstore_block = cstore_offset_to_cstoreblock(offset, align_size);
    return (cstore_block / BCM_BLOCKS_PER_PAGE) + UNITBLK_TO_BCMGROUP(cstore_block) + 2;
}
