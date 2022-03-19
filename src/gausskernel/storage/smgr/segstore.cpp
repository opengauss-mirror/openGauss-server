/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * segstore.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segstore.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xloginsert.h"
#include "access/xlogproc.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/aiocompleter.h"
#include "storage/buf/bufmgr.h"
#include "storage/procarray.h"
#include "storage/smgr/segment.h"
#include "storage/smgr/smgr.h"
#include "utils/resowner.h"
#include "vectorsonic/vsonichash.h"
#include "storage/procarray.h"
/*
 * This code manages relations that reside on segment-page storage. It implements functions used for smgr.cpp.
 *
 * Relation forks
 * ==============
 * In magnetic disk storage (md.c), each fork is an individual physical file, storing different metadata of the same
 * relation. The file name is comprised by relation's relfilenode and fork number. For example, the Free Space fork
 * of a relation with relfilenode 16384 has the file name 16384_fsm. Thus, one relation has one SMgrRelation object.
 * Different forks share the same relflienode (i.e., the same prefix of file name), and their file handlers are
 * stored in an array named md_fd.
 *
 * In segment-page storage, the relfilenode of a relation is its main fork's segment head block number. And each fork
 * has an individual segment, routed by its segment head block number. To support adding new forks in the future, we
 * store forks' segment head location in main fork's segment head (see definition of SegmentHead). To be compatible
 * with previous systems, one relation also has one SMgrRelation object in segment-page storage. Each fork's
 * segment head is cached in the array named "seg_space". When the first time a backend worker accessing a non-main
 * fork, it would find this fork's segment head number in the main fork's segment head, and store it in the
 * SMgrRelation, so that it can skip searching next time.
 *
 * SMgrRelation is cached in local hashtable to reduce frequent open/close costs. The hash key contains relfilenode.
 * Different backends have different SMgrRelation objects. In previous magnetic disk storage, it does not have to
 * synchronize among backends. Because given the same relfilenode + forknumber, they can have the same physical
 * file name. The file system will synchronize for them.
 *
 * But in segment-page storage, relfilenode is the block number of the main fork's segment head. Given a relfilenode +
 * forknumber (forknumber != MAIN_FORKNUM), we do not know this fork's location. We have to search it first, and
 * then store it in SMgrRelation.seg_desc. If the relationship between a main fork and a non-main fork changes,
 * the local SMgrRelation should be invalidated. See scenarios as below:
 *
 * Case 1:  Given a relation with main fork's segment head being X, FSM fork's segment head being Y. The bgwriter thread
 *          wants to flush one buffer, so generating a SMgrRelation 'reln':
 *              reln->smgr_rnode.relfilenode = X, and reln->seg_desc[FSM_FORKNUM].head_blocknum = Y.
 *
 *          Later, another backend thread, drops the relation, and then generates another relation with main fork's
 *          segment head being X (the dropped segment can be reused), FSM fork's segment head being Z. It is possible
 *          because we allocate different forks' segments independently. Now, the bgwriter wants to flush a new dirty
 *          buffer, it would use the existing SMgrRelation object 'reln' (bgwriter does not AcceptInvalidationMessages
 *          so SMgrRelation objects are never invalidated), and write the data to segment Y, which should be written
 *          to segment Z actually.
 *
 * Case 2:  Given a relation with main fork's segment head being X, FSM fork's segment head being Y. Another backend
 *          calls "space shrink" and moves the FSM fork to segment Z. But local SMgrRelation object still points to
 *          segment Y.
 *
 * Thus, we need an synchronization mechanism, to find whether the head_blocknum cached in SMgrRelation is consistent.
 * If not, it should invalidate current cache, and re-search from the main fork's segment head.
 *
 * The solution is that we add a global timeline. When a SMgrRelation is created, it records current timeline. When
 * we 'unlink' a fork's segment, we add the timeline by one. Each time before we using a segment head to do any read
 * or write, we check whether the timeline recorded in the current SMgrRelation object is equal to the global timeline.
 * Not equal means the risk of inconsistence, and we should update the SMgrRelation cache. The solution seem to be too
 * aggressive to invalidate all SMgrRelation objects. But 'unlink' is not frequent in real system.
 * Currently, all cases are listed as follow:
 *      1. drop table.
 *      2. delete a list of buckets during redistributing.
 */
static void seg_update_timeline()
{
    pg_atomic_add_fetch_u32(&g_instance.segment_cxt.segment_drop_timeline, 1);
}

static uint32 seg_get_drop_timeline()
{
    uint32 expected = 0;
    pg_atomic_compare_exchange_u32(&g_instance.segment_cxt.segment_drop_timeline, &expected, 0);
    return expected;
}

struct SegmentHeadLockPartitionTag {
    Oid spcNode;
    Oid dbNode;
    BlockNumber head;
};

void LockSegmentHeadPartition(Oid spcNode, Oid dbNode, BlockNumber head, LWLockMode mode)
{
    SegmentHeadLockPartitionTag tag = {.spcNode = spcNode, .dbNode = dbNode, head = head};
    uint32 hashcode = hashquickany(0xFFFFFFFF, (unsigned char *)&tag, sizeof(tag));
    LWLock *lock = SegmentHeadPartitionLock(hashcode);
    LWLockAcquire(lock, mode);
}

void UnlockSegmentHeadPartition(Oid spcNode, Oid dbNode, BlockNumber head)
{
    SegmentHeadLockPartitionTag tag = {.spcNode = spcNode, .dbNode = dbNode, head = head};
    uint32 hashcode = hashquickany(0xFFFFFFFF, (unsigned char *)&tag, sizeof(tag));
    LWLock *lock = SegmentHeadPartitionLock(hashcode);
    LWLockRelease(lock);
}

#define IsBucketSMgrRelation(reln) IsBucketFileNode((reln)->smgr_rnode.node)

#define ReadSegmentBuffer(spc, blockno)                                                                                \
    ReadBufferFast((spc), EXTENT_GROUP_RNODE((spc), SEGMENT_HEAD_EXTENT_SIZE), MAIN_FORKNUM, (blockno), RBM_NORMAL)

#define ReadLevel0Buffer(spc, blockno)                                                                                 \
    ReadBufferFast((spc), EXTENT_GROUP_RNODE((spc), LEVEL0_PAGE_EXTENT_SIZE), MAIN_FORKNUM, (blockno), RBM_NORMAL)

/*
 * Calculate extent id & offset according to logic page id
 * input: logic_id
 * output: extent_id, offset, extent_size
 */
inline static void SegLogicPageIdToExtentId(BlockNumber logic_id, uint32 *extent_id, uint32 *offset,
                                            ExtentSize *extent_size)
{
    if (logic_id < EXT_SIZE_8_TOTAL_PAGES) {
        *extent_size = EXT_SIZE_8;
        *extent_id = logic_id / EXT_SIZE_8;
        *offset = logic_id % EXT_SIZE_8;
    } else if (logic_id < EXT_SIZE_128_TOTAL_PAGES) {
        *extent_size = EXT_SIZE_128;
        logic_id -= EXT_SIZE_8_TOTAL_PAGES;
        *extent_id = EXT_SIZE_8_BOUNDARY + logic_id / EXT_SIZE_128;
        *offset = logic_id % EXT_SIZE_128;
    } else if (logic_id < EXT_SIZE_1024_TOTAL_PAGES) {
        *extent_size = EXT_SIZE_1024;
        logic_id -= EXT_SIZE_128_TOTAL_PAGES;
        *extent_id = EXT_SIZE_128_BOUNDARY + logic_id / EXT_SIZE_1024;
        *offset = logic_id % EXT_SIZE_1024;
    } else {
        *extent_size = EXT_SIZE_8192;
        logic_id -= EXT_SIZE_1024_TOTAL_PAGES;
        *extent_id = EXT_SIZE_1024_BOUNDARY + logic_id / EXT_SIZE_8192;
        *offset = logic_id % EXT_SIZE_8192;
    }
}

void log_move_segment_buckets(xl_seg_bktentry_tag_t *mapentry, uint32 nentry, Buffer buffer)
{
    XLogAtomicOpRegisterBuffer(buffer, REGBUF_NO_IMAGE, SPCXLOG_SEG_MOVE_BUCKETS, XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
    XLogAtomicOpRegisterBufData((char *)&nentry, sizeof(uint32));
    XLogAtomicOpRegisterBufData((char *)mapentry, sizeof(xl_seg_bktentry_tag_t) * nentry);
}

void log_move_segment_redisinfo(SegRedisInfo *dredis, SegRedisInfo *sredis, Buffer dbuffer, Buffer sbuffer)
{
    XLogAtomicOpRegisterBuffer(dbuffer, REGBUF_NO_IMAGE, SPCXLOG_BUCKET_ADD_REDISINFO, XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
    XLogAtomicOpRegisterBufData((char *)dredis, sizeof(SegRedisInfo));

    XLogAtomicOpRegisterBuffer(sbuffer, REGBUF_NO_IMAGE, SPCXLOG_BUCKET_ADD_REDISINFO, XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
    XLogAtomicOpRegisterBufData((char *)sredis, sizeof(SegRedisInfo));
}

RelFileNode EXTENT_GROUP_RNODE(SegSpace *spc, ExtentSize extentSize)
{
    return {.spcNode = spc->spcNode,
            .dbNode = spc->dbNode,
            .relNode = EXTENT_SIZE_TO_TYPE(extentSize),
            .bucketNode = SegmentBktId,
            .opt = 0};
}

void seg_head_update_xlog(Buffer head_buffer, SegmentHead *seg_head, int level0_slot,
                          int level1_slot)
{
    XLogDataUpdateSegmentHead xlog_data;
    errno_t rc = memset_s(&xlog_data, sizeof(xlog_data), 0, sizeof(xlog_data));
    securec_check(rc, "\0", "\0");

    xlog_data.level0_slot = level0_slot;
    xlog_data.level1_slot = level1_slot;
    if (level0_slot >= 0) {
        xlog_data.level0_value = seg_head->level0_slots[level0_slot];
    } else {
        xlog_data.level0_value = InvalidBlockNumber;
    }

    if (level1_slot >= 0) {
        xlog_data.level1_value = seg_head->level1_slots[level1_slot];
    } else {
        xlog_data.level1_value = InvalidBlockNumber;
    }

    // just copy nblocks, total_blocks and nextents
    xlog_data.nblocks = seg_head->nblocks;
    xlog_data.total_blocks = seg_head->total_blocks;
    xlog_data.nextents = seg_head->nextents;

    // We do not want XLogAtomicOp release or unlock buffer for us.
    XLogAtomicOpRegisterBuffer(head_buffer, REGBUF_KEEP_DATA, SPCXLOG_UPDATE_SEGHEAD, XLOG_COMMIT_KEEP_BUFFER_STATE);
    XLogAtomicOpRegisterBufData((char *)&xlog_data, sizeof(xlog_data));
}

void seg_init_new_level0_page(SegSpace *spc, uint32_t new_extent_id, Buffer seg_head_buffer,
                              BlockNumber new_level0_page, BlockNumber first_extent)
{
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetPage(seg_head_buffer));

    Buffer new_level0_buffer = ReadLevel0Buffer(spc, new_level0_page);
    LockBuffer(new_level0_buffer, BUFFER_LOCK_EXCLUSIVE);
    Page level0_page = BufferGetPage(new_level0_buffer);

    SegPageInit(level0_page, BLCKSZ);
    PageHeader header = (PageHeader)level0_page;
    header->pd_lower += sizeof(BMTLevel0Page);

    BMTLevel0Page *bmt_level0_page = (BMTLevel0Page *)PageGetContents(level0_page);
    bmt_level0_page->slots[0] = first_extent;
    bmt_level0_page->magic = BMTLEVEL0_MAGIC;

    uint32 level1_slot = ExtentIdToLevel1Slot(new_extent_id);
    seg_head->level1_slots[level1_slot] = new_level0_page;

    // XLog issue
    {
        // new level0 page, we do not use level0 page buffer anymore, release it.
        XLogAtomicOpRegisterBuffer(new_level0_buffer, REGBUF_WILL_INIT | REGBUF_KEEP_DATA, SPCXLOG_NEW_LEVEL0_PAGE,
                                   XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
        XLogAtomicOpRegisterBufData((char *)&first_extent, sizeof(BlockNumber));

        // segment head, only modify level1_slot.
        seg_head_update_xlog(seg_head_buffer, seg_head, -1, level1_slot);
    }
}

void seg_record_new_extent_on_level0_page(SegSpace *spc, Buffer seg_head_buffer, uint32 new_extent_id,
                                          BlockNumber new_extent_first_pageno)
{
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetPage(seg_head_buffer));

    uint32 level1_slot = ExtentIdToLevel1Slot(new_extent_id);
    BlockNumber level0_pageno = seg_head->level1_slots[level1_slot];
    uint32 level0_offset = ExtentIdToLevel0PageOffset(new_extent_id);

    Buffer level0_buffer = ReadLevel0Buffer(spc, level0_pageno);
    BMTLevel0Page *bmt_level0_page = (BMTLevel0Page *)PageGetContents(BufferGetPage(level0_buffer));
    LockBuffer(level0_buffer, BUFFER_LOCK_EXCLUSIVE);

    bmt_level0_page->slots[level0_offset] = new_extent_first_pageno;

    // XLogIssue
    {
        // level0 page, we do not use level0 page buffer anymore, release it.
        XLogDataSetLevel0Page xlog_data;
        xlog_data.slot = level0_offset;
        xlog_data.extent = new_extent_first_pageno;
        XLogAtomicOpRegisterBuffer(level0_buffer, REGBUF_KEEP_DATA, SPCXLOG_LEVEL0_PAGE_ADD_EXTENT,
                                   XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
        XLogAtomicOpRegisterBufData((char *)&xlog_data, sizeof(XLogDataSetLevel0Page));
    }
}

/* internal function */

/*
 * Add one extent for a given segment.
 *
 * This function is an atomic operation, i.e., extent allocation and segment head metadata update are orgainized
 * into one xlog. Thus, extents won't be leaked even if crash happens during the function execution.
 *
 * Note: segment head buffer is locked before invoking the function, and is unlocked before return.
 */
void seg_extend_segment(SegSpace *spc, ForkNumber forknum, Buffer seg_head_buffer, BlockNumber seg_head_blocknum)
{
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetPage(seg_head_buffer));
    uint32 extent_size = ExtentSizeByCount(seg_head->nextents);

    ExtentInversePointer iptr = {.flag = SPC_INVRSPTR_ASSEMBLE_FLAG(DATA_EXTENT, seg_head->nextents),
                                 .owner = seg_head_blocknum};

    XLogAtomicOpStart();
    BlockNumber new_extent = spc_alloc_extent(spc, extent_size, forknum, InvalidBlockNumber, iptr);

    START_CRIT_SECTION();

    uint32 new_extent_id = seg_head->nextents;
    seg_head->nextents++;
    seg_head->total_blocks += extent_size;

    if (new_extent_id < BMT_HEADER_LEVEL0_SLOTS) {
        seg_head->level0_slots[new_extent_id] = new_extent;

        // XLog issue, only update segment head, level0 slot is modified.
        seg_head_update_xlog(seg_head_buffer, seg_head, new_extent_id, -1);
    } else {
        if (RequireNewLevel0Page(new_extent_id)) {
            BlockNumber level0_page =
                spc_alloc_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, InvalidBlockNumber, iptr);
            seg_init_new_level0_page(spc, new_extent_id, seg_head_buffer, level0_page, new_extent);
        } else {
            seg_record_new_extent_on_level0_page(spc, seg_head_buffer, new_extent_id, new_extent);
            /* segment head, does not modify any level0 or level1 slot; only modify "nextents" and "total_blocks" */
            seg_head_update_xlog(seg_head_buffer, seg_head, -1, -1);
        }
    }
    SEGMENTTEST(SEG_STORE_EXTEND_EXTENT, (errmsg("SEG_STORE_EXTEND_EXTENT %s: segment extend an extent!\n",
        g_instance.attr.attr_common.PGXCNodeName)));
    XLogAtomicOpCommit();

    END_CRIT_SECTION();
}

BlockNumber seg_extent_location(SegSpace *spc, SegmentHead *seg_head, int extent_id)
{
    if (extent_id < BMT_HEADER_LEVEL0_SLOTS) {
        return seg_head->level0_slots[extent_id];
    } else {
        BlockNumber level0_page_id = ExtentIdToLevel0PageNumber(seg_head, extent_id);
        Buffer buffer = ReadLevel0Buffer(spc, level0_page_id);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        BMTLevel0Page *level0_page = (BMTLevel0Page *)PageGetContents(BufferGetPage(buffer));

        extent_id -= BMT_HEADER_LEVEL0_SLOTS;
        BlockNumber start = level0_page->slots[extent_id % BMT_LEVEL0_SLOTS];

        SegUnlockReleaseBuffer(buffer);

        return start;
    }
}

SegPageLocation seg_logic_to_physic_mapping(SMgrRelation reln, SegmentHead *seg_head, BlockNumber logic_id)
{
    uint32 extent_id;
    uint32 offset;
    ExtentSize extent_size;
    BlockNumber blocknum;

    /* Recovery thread should use physical location to read data directly. */
    if (RecoveryInProgress() && !CurrentThreadIsWorker()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("cannot do segment address translation during recovery")));
    }

    SegLogicPageIdToExtentId(logic_id, &extent_id, &offset, &extent_size);

    BlockNumber extent_start = seg_extent_location(reln->seg_space, seg_head, extent_id);
    blocknum = extent_start + offset;

    return {
        .extent_size = extent_size,
        .extent_id = extent_id,
        .blocknum = blocknum,
    };
}

/*
 * Segment Head Basic Function
 *
 * Normal tables and hashbucket tables have totally different implementations about segment head. Thus,
 * we provide a set of segment head related functions to hide implementation details for upper layers.
 *
 * alloc_segment:
 *      allocate a segment. A normal table allocates an extent and use the first block as segment head.
 *      A hashbucket table needs to initialize head map entries. Segment head for buckets is not allocated
 *      in this period.
 * open_segment:
 *      open a segment means setting some values in the SMgrRelationData so that we can use next time.
 *      Currently, we only record the block number of the segment head.
 * read_head_buffer:
 *      load the segment head into buffer
 */
static Buffer read_head_buffer(SMgrRelation reln, ForkNumber forknum, bool create);
static bool open_segment(SMgrRelation reln, ForkNumber forknum, bool create, XLogRecPtr lsn = InvalidXLogRecPtr);

/* Normal Table Segment Head API */
static bool normal_open_segment(SMgrRelation reln, int forknum, bool create);
static BlockNumber normal_alloc_segment(Oid tablespace_id, Oid database_id, BlockNumber preassigned_block,
                                        ExtentInversePointer iptr, bool is_heap_seg_head=false);

void eg_init_segment_head_buffer_content(Buffer seg_head_buffer, BlockNumber seg_head_blocknum, XLogRecPtr lsn)
{
    SegPageInit(BufferGetPage(seg_head_buffer), BLCKSZ);

    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetPage(seg_head_buffer));
    seg_head->magic = SEGMENT_HEAD_MAGIC;
    seg_head->lsn = lsn;
    seg_head->nextents = 0;
    seg_head->nblocks = 0;
    seg_head->total_blocks = 0;
    seg_head->bucketid = InvalidBktId;
    seg_head->nslots = BMT_TOTAL_SLOTS;
    seg_head->level0_slots[0] = seg_head_blocknum;

    /* set segment head of other forks as invalid; they will be created later if users require. */
    for (int i = MAIN_FORKNUM + 1; i <= SEGMENT_MAX_FORKNUM; i++) {
        seg_head->fork_head[i] = InvalidBlockNumber;
    }
    seg_head->fork_head[MAIN_FORKNUM] = seg_head_blocknum;
    ((PageHeader)BufferGetPage(seg_head_buffer))->pd_lower += sizeof(SegmentHead);
}

void eg_init_segment_head(SegSpace *spc, BlockNumber seg_head_blocknum, bool is_heap_seg_head)
{
    /* load the segment head into buffer and initialize it */
    Buffer buffer = ReadBufferFast(spc, EXTENT_GROUP_RNODE(spc, SEGMENT_HEAD_EXTENT_SIZE), MAIN_FORKNUM,
                                   seg_head_blocknum, RBM_ZERO_AND_LOCK);
    XLogRecPtr lsn = GetXLogInsertRecPtr();
    eg_init_segment_head_buffer_content(buffer, seg_head_blocknum, lsn);

    XLogAtomicOpRegisterBuffer(buffer, REGBUF_WILL_INIT, SPCXLOG_INIT_SEGHEAD, XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
    XLogAtomicOpRegisterBufData((char *)&lsn, sizeof(XLogRecPtr));

    /* As sizeof(bool) is undefined, we translate it to an unsigned char variable. */
    unsigned char is_heap_head_v = is_heap_seg_head ? 1 : 0;
    XLogAtomicOpRegisterBufData((char *)&is_heap_head_v, sizeof(is_heap_head_v));
}

/*
 * Allocate a segment and return the block number of the segment head.
 *
 * We use spcNode and dbNode to find the segment space, thus must make sure that these two variables are set
 * correctly before invoking this function. Other variables are ignored.
 * Segment head will be initialized, and related buffer will be released before this function returns.
 *
 * normal_alloc_segment is not an atomic operation itself, it may be one step of an atomic operation.
 * For example, creating a non-main fork segment, needs to (1) allocate a normal segment, and then (2) link
 * it to the main-fork head.
 */
static BlockNumber normal_alloc_segment(Oid tablespace_id, Oid database_id, BlockNumber preassigned_block,
                                        ExtentInversePointer iptr, bool is_heap_seg_head)
{
    /* Open the segment space, and allocate a 8-block extent*/
    SegSpace *spc = spc_open(tablespace_id, database_id, true);
    SegmentCheck(spc);

    BlockNumber seg_head_blocknum =
        spc_alloc_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, preassigned_block, iptr);
    eg_init_segment_head(spc, seg_head_blocknum, is_heap_seg_head);

    return seg_head_blocknum;
}

static bool normal_open_segment(SMgrRelation reln, int forknum, bool create)
{
    BlockNumber fork_head_blocknum = InvalidBlockNumber;
    Buffer main_buffer;
    SegmentHead *main_head;

    if (forknum == MAIN_FORKNUM) {
        /* For the main fork, the variable relfilenode is the segment head block number */
        fork_head_blocknum = reln->smgr_rnode.node.relNode;
        goto CREATE_DESC;
    }

    /* Ensure the main fork is opened */
    open_segment(reln, MAIN_FORKNUM, false);
    main_buffer = ReadSegmentBuffer(reln->seg_space, reln->seg_desc[MAIN_FORKNUM]->head_blocknum);
    main_head = (SegmentHead *)PageGetContents(BufferGetBlock(main_buffer));
    if (unlikely(!IsNormalSegmentHead(main_head))) {
        ereport(PANIC, (errmodule(MOD_SEGMENT_PAGE), errmsg("Segment head magic value 0x%lx is invalid,"
            "head lsn 0x%lx(maybe wrong). Rnode [%u, %u, %u, %d], head blocknum %u.",
            main_head->magic, main_head->lsn, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
            reln->smgr_rnode.node.relNode, reln->smgr_rnode.node.bucketNode,
            reln->seg_desc[MAIN_FORKNUM]->head_blocknum)));
    }

    /*
     * For non-main fork, the segment head is stored in the main fork segment head.
     * The block number being invalid means that the segment has not been created yet.
     */
    fork_head_blocknum = main_head->fork_head[forknum];

    if (fork_head_blocknum == InvalidBlockNumber) {
        if (create) {
            LockBuffer(main_buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * Someone else may allocate a segment head for this fork before we get the exclusive lock.
             * Thus we must check it again.
             */
            fork_head_blocknum = main_head->fork_head[forknum];
            if (fork_head_blocknum == InvalidBlockNumber) {
                /*
                 * An atomic operation: segment allocation and linking the head to main segment
                 */
                ExtentInversePointer iptr = {.flag = SPC_INVRSPTR_ASSEMBLE_FLAG(FORK_HEAD, forknum),
                                             .owner = reln->smgr_rnode.node.relNode};
                XLogAtomicOpStart();
                BlockNumber new_head_blocknum = normal_alloc_segment(
                    reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode, InvalidBlockNumber, iptr);

                XLogAtomicOpRegisterBuffer(main_buffer, REGBUF_KEEP_DATA, SPCXLOG_SEGHEAD_ADD_FORK_SEGMENT,
                                           XLOG_COMMIT_KEEP_BUFFER_STATE);
                XLogAtomicOpRegisterBufData((char *)&forknum, sizeof(int));
                XLogAtomicOpRegisterBufData((char *)&new_head_blocknum, sizeof(BlockNumber));

                main_head->fork_head[forknum] = new_head_blocknum;
                fork_head_blocknum = new_head_blocknum;

                XLogAtomicOpCommit();
                SEGMENTTEST(FREE_EXTENT_ADD_FSM_FORK, (errmsg("FREE_EXTENT_ADD_FSM_FORK %s: add fsm fork, free extent success!\n",
                    g_instance.attr.attr_common.PGXCNodeName)));

                ereport(DEBUG5,
                        (errmodule(MOD_SEGMENT_PAGE), errmsg("Add fork %d for segment %u, fork head is %u", forknum,
                                                             reln->smgr_rnode.node.relNode, fork_head_blocknum)));
            }
            SegUnlockReleaseBuffer(main_buffer);
        } else {
            SegReleaseBuffer(main_buffer);
            return false;
        }
    } else {
        SegReleaseBuffer(main_buffer);
    }

CREATE_DESC:
    /*
     * Initialize the segment descriptor in SMgrRelationData.
     */
    SegmentDesc *fork_desc =
        (SegmentDesc *)MemoryContextAlloc(LocalSmgrStorageMemoryCxt(), sizeof(SegmentDesc));
    fork_desc->head_blocknum = fork_head_blocknum;
    fork_desc->timeline = seg_get_drop_timeline();
    SegmentCheck(fork_head_blocknum >= DF_MAP_GROUP_SIZE);
    reln->seg_desc[forknum] = fork_desc;

    return true;
}

/*
 * This function is the smallest atomic unit during remove a segment.
 * It frees the last extent in the segment, and sets meta data in the segment head.
 * All these steps are packed in one XLog.
 *
 * head_buffer is locked outside the function.
 */
static void free_last_extent(SegSpace *spc, ForkNumber forknum, Buffer head_buffer)
{
    SegmentHead *head = (SegmentHead *)(PageGetContents(BufferGetPage(head_buffer)));
    SegmentCheck(head->nextents > 0);

    /*
     * We do not care content on Level0 Page. Because once we update "nextents" in
     * SegmentHead, these extents managed by the level0 page are safely freed.
     */
    int last_ext_id = head->nextents - 1;
    BlockNumber ext_blk = seg_extent_location(spc, head, last_ext_id);
    ExtentSize ext_size = ExtentSizeByCount(last_ext_id);

    XLogAtomicOpStart();
    spc_free_extent(spc, ext_size, forknum, ext_blk);

    /* We modify the content in the buffer at first, so do not allow any ERROR happens until end */
    START_CRIT_SECTION();
    head->nextents--;
    head->total_blocks -= ext_size;

    if (RequireNewLevel0Page(last_ext_id)) {
        // free last level0page, the level0 page belongs to the MAIN_FORK.
        uint32 level1_slot = ExtentIdToLevel1Slot(last_ext_id);
        BlockNumber extent = head->level1_slots[level1_slot];
        spc_free_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, extent);
        head->level1_slots[level1_slot] = InvalidBlockNumber;

        seg_head_update_xlog(head_buffer, head, -1, level1_slot);
    } else {
        /* Only updates nextents and total_blocks */
        seg_head_update_xlog(head_buffer, head, -1, -1);
    }

    END_CRIT_SECTION();

    SEGMENTTEST(FREE_EXTENT_DROP_EXTENTS, (errmsg("FREE_EXTENT_DROP_EXTENTS %s: "
        "drop some extents,remain extents can drop !\n", g_instance.attr.attr_common.PGXCNodeName)));
    XLogAtomicOpCommit();

     /* If all extents is freed, flush segment header to disk */
    if (head->total_blocks == 0) {
        SegmentCheck(head->nextents == 0);
        FlushOneSegmentBuffer(head_buffer);
    }
}

static void free_segment_extents(SegSpace *spc, ForkNumber forknum, Buffer head_buffer)
{
    SegmentHead *head = (SegmentHead *)(PageGetContents(BufferGetPage(head_buffer)));
    while (head->nextents > 0) {
        free_last_extent(spc, forknum, head_buffer);
    }
}

/*
 * The pattern for free segment is almost the same, thus we extract a single function.
 * The procedure is as follow:
 * 1. Free extents in the segment on by one. Each time we free the last extent and reduce "nextents" in segment
 *    head by 1. Each step is atomic, and generates one XLog. If system crashes at any point, the recovery procedure
 *    can continue to free the rest of extents, as long as it knows the segment head.
 * 2. In the end, there is only one extent in the segment not freed, i.e., the extent contains the segment head.
 *    There must be an "owner" that points to this segment. For example, if the segment is used for a non-main fork,
 *    the owner is "fork_head" in the main fork segment head; if the segment is used for a bucket, the owner is
 *    "head_block" in the corresponding MapBlock. Free the last extent and reset the owner pointer are wrapped in
 *    one atomic step, generating one XLog.
 *
 * The parameter
 *    segment_buffer: represents the buffer of the segment head to be freed. It should be pinned, but
 * not locked before invoking this function.
 *    owner_buffer: represents the buffer containing the owner pointer. It should be locked exclusive before
 * invocation, and will be marked as dirty within this function.
 *    offset: means the the owner pointer's location in the owner buffer. The page head size is not calculated by the
 * offset.
 *
 */
static void free_one_segment(SegSpace *spc, ForkNumber forknum, Buffer segment_buffer, BlockNumber head_blocknum,
                             Buffer owner_buffer, off_t offset)
{
    /* Free all extents in the segment except the segment head block*/
    free_segment_extents(spc, forknum, segment_buffer);

    /* Free the segment head block, and set owner's pointer as InvalidBlockNumber */
    XLogAtomicOpStart();
    spc_free_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, head_blocknum);

    /* Remove it from the owner buffer */
    BlockNumber *owner_pointer = (BlockNumber *)((char *)PageGetContents(BufferGetPage(owner_buffer)) + offset);
    *owner_pointer = InvalidBlockNumber;

    /* Owner buffer is unlocked and unpinned by caller */
    XLogAtomicOpRegisterBuffer(owner_buffer, REGBUF_KEEP_DATA, SPCXLOG_UNLINK_SEGHEAD_PTR,
                               XLOG_COMMIT_KEEP_BUFFER_STATE);
    XLogAtomicOpRegisterBufData((char *)&offset, sizeof(offset));
    XLogAtomicOpCommit();
}

static void normal_unlink_segment(SegSpace *spc, const RelFileNode &rnode, Buffer main_head_buffer)
{
    SegmentHead *main_head = (SegmentHead *)PageGetContents(BufferGetBlock(main_head_buffer));
    SegmentCheck(IsNormalSegmentHead(main_head));

    LockBuffer(main_head_buffer, BUFFER_LOCK_EXCLUSIVE);

    /* we must recycle main fork in the end */
    for (int i = SEGMENT_MAX_FORKNUM; i > MAIN_FORKNUM; i--) {
        BlockNumber fork_head = main_head->fork_head[i];
        if (BlockNumberIsValid(fork_head)) {
            Buffer buffer = ReadSegmentBuffer(spc, fork_head);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

            off_t offset = offsetof(SegmentHead, fork_head) + sizeof(BlockNumber) * i;

            free_one_segment(spc, i, buffer, fork_head, main_head_buffer, offset);
            SegUnlockReleaseBuffer(buffer);
        }
    }

    /* free extents under main fork */
    free_segment_extents(spc, MAIN_FORKNUM, main_head_buffer);

    XLogAtomicOpStart();
    spc_free_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, rnode.relNode);
    XLogAtomicOpCommit();

    LockBuffer(main_head_buffer, BUFFER_LOCK_UNLOCK);
}

/* Bucket Segment Head API */
static BlockNumber bucket_alloc_segment(Oid tablespace_id, Oid database_id, BlockNumber preassigned_block);
static bool bucket_open_segment(SMgrRelation reln, int forknum, bool create, XLogRecPtr lsn);

/*
 * main_buffer should be locked outside
 */
static void bucket_ensure_mapblock(SegSpace *spc, Buffer main_buffer, uint32 blockid, XLogRecPtr lsn)
{
    BktMainHead *main_head = (BktMainHead *)PageGetContents(BufferGetPage(main_buffer));
    SegmentCheck(IsBucketMainHead(main_head));
    if (main_head->bkt_map[blockid] == 0) {
        // BktHeadMapBlock is not initialized.
        BufferDesc *buf_desc = GetBufferDescriptor(main_buffer - 1);
        BlockNumber main_head_blocknum = buf_desc->tag.blockNum;

        XLogAtomicOpStart();

        ExtentInversePointer iptr = {.flag = SPC_INVRSPTR_ASSEMBLE_FLAG(BUCKET_MAP, blockid),
                                     .owner = main_head_blocknum};
        BlockNumber newmap_block =
            spc_alloc_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, InvalidBlockNumber, iptr);
        main_head->bkt_map[blockid] = newmap_block;

        Buffer map_buffer = ReadSegmentBuffer(spc, newmap_block);
        LockBuffer(map_buffer, BUFFER_LOCK_EXCLUSIVE);
        bucket_init_map_page(map_buffer, lsn);
        XLogAtomicOpRegisterBuffer(map_buffer, REGBUF_WILL_INIT, SPCXLOG_BUCKET_INIT_MAPBLOCK,
                                   XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
        XLogAtomicOpRegisterBufData((char *)&lsn, sizeof(XLogRecPtr));

        XLogAtomicOpRegisterBuffer(main_buffer, REGBUF_KEEP_DATA, SPCXLOG_BUCKET_ADD_MAPBLOCK,
                                   XLOG_COMMIT_KEEP_BUFFER_STATE);
        XLogAtomicOpRegisterBufData((char *)&blockid, sizeof(uint32));
        XLogAtomicOpRegisterBufData((char *)&newmap_block, sizeof(BlockNumber));

        XLogAtomicOpCommit();
    }
}
/*
 * Given a bucket id, calculate the position of its map entry. The main head does not require any lock
 * at first try, because the map entry position is static after it is initialized.
 *
 * input:
 *  - main_buffer
 *  - bucketid
 *  - forknum
 * output:
 *  - map_blocknum
 *  - map_entry_id
 */
static void bucket_get_mapentry(BktMainHead *main_head, int4 bucketid, int forknum, BlockNumber *map_blocknum,
                                int *map_entry_id)
{
    SegmentCheck(IsBucketMainHead(main_head));
    int k = forknum * MAX_BUCKETMAPLEN + bucketid;
    int blockid = k / BktMapEntryNumberPerBlock;
    *map_entry_id = k % BktMapEntryNumberPerBlock;
    *map_blocknum = main_head->bkt_map[blockid];
    SegmentCheck(*map_blocknum != 0);
}

void bucket_init_map_page(Buffer map_buffer, XLogRecPtr lsn)
{
    Page map_page = BufferGetPage(map_buffer);
    SegPageInit(map_page, BLCKSZ);

    BktHeadMapBlock *map_block = (BktHeadMapBlock *)PageGetContents(map_page);
    map_block->magic = BUCKETMAP_MAGIC;
    map_block->lsn = lsn;
    for (uint32 j = 0; j < BktMapEntryNumberPerBlock; j++) {
        map_block->head_block[j] = InvalidBlockNumber;
    }
    ((PageHeader)map_page)->pd_lower += sizeof(BktHeadMapBlock);
}

static BlockNumber bucket_alloc_segment(Oid tablespace_id, Oid database_id, BlockNumber preassigned_block)
{
    SegSpace *spc = spc_open(tablespace_id, database_id, true);

    ExtentInversePointer iptr = {.flag = SPC_INVRSPTR_ASSEMBLE_FLAG(BUCKET_SEGMENT, 0), .owner = InvalidBlockNumber};

    /* Allocate BktMainHead first */
    XLogAtomicOpStart();
    BlockNumber main_head_blocknum =
        spc_alloc_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, InvalidBlockNumber, iptr);
    Buffer main_head_buffer = ReadSegmentBuffer(spc, main_head_blocknum);
    LockBuffer(main_head_buffer, BUFFER_LOCK_EXCLUSIVE);

    Page main_head_page = BufferGetPage(main_head_buffer);
    SegPageInit(main_head_page, BLCKSZ);
    ((PageHeader)main_head_page)->pd_lower += sizeof(BktMainHead);

    BktMainHead *main_head = (BktMainHead *)PageGetContents(main_head_page);
    XLogRecPtr lsn = GetXLogInsertRecPtr();
    main_head->magic = BUCKET_SEGMENT_MAGIC;
    main_head->lsn = lsn;
    main_head->redis_info.redis_xid = InvalidTransactionId;
    main_head->redis_info.nwords = 0;

    XLogAtomicOpRegisterBuffer(main_head_buffer, REGBUF_WILL_INIT, SPCXLOG_INIT_BUCKET_HEAD,
                               XLOG_COMMIT_KEEP_BUFFER_STATE);
    XLogAtomicOpRegisterBufData((char *)&lsn, sizeof(XLogRecPtr));
    XLogAtomicOpCommit();

    for (uint32 i = 0; i < BktMapBlockNumber; i++) {
        bucket_ensure_mapblock(spc, main_head_buffer, i, lsn);
    }

    SegUnlockReleaseBuffer(main_head_buffer);

    return main_head_blocknum;
}

static inline void bucket_load_main_head(SegSpace *spc, BlockNumber blocknum, Buffer *buffer, BktMainHead **head)
{
    *buffer = ReadSegmentBuffer(spc, blocknum);
    *head = (BktMainHead *)PageGetContents(BufferGetPage(*buffer));
}

static bool bucket_open_segment(SMgrRelation reln, int forknum, bool create, XLogRecPtr lsn)
{
    if (reln->seg_desc[forknum] != NULL) {
        return true;
    }
    Buffer main_buffer;
    BktMainHead *main_head;
    bucket_load_main_head(reln->seg_space, reln->smgr_rnode.node.relNode, &main_buffer, &main_head);
    SegmentCheck(IsBucketMainHead(main_head));
    int4 bucketid = reln->smgr_rnode.node.bucketNode;

    /* find the map block */
    BlockNumber map_blocknum;
    int map_entry_id;
    bucket_get_mapentry(main_head, bucketid, forknum, &map_blocknum, &map_entry_id);

    if (map_blocknum == InvalidBlockNumber)
    {
        SegmentCheck(XLogRecPtrIsValid(lsn));
        SegReleaseBuffer(main_buffer);
        return false;
    }

    /* get the entry */
    Buffer map_buffer = ReadSegmentBuffer(reln->seg_space, map_blocknum);
    LockBuffer(map_buffer, BUFFER_LOCK_SHARE);
    BktHeadMapBlock *map_block = (BktHeadMapBlock *)PageGetContents(BufferGetPage(map_buffer));

    if (XLogRecPtrIsValid(lsn) && (XLByteLE(lsn, map_block->lsn) || map_block->magic != BUCKETMAP_MAGIC)) {
        SegUnlockReleaseBuffer(map_buffer);
        SegReleaseBuffer(main_buffer);
        return false;
    }

    BlockNumber head_blocknum = map_block->head_block[map_entry_id];

    if (head_blocknum == InvalidBlockNumber) {
        /* the entry is not initialized */
        if (create == false) {
            SegUnlockReleaseBuffer(map_buffer);
            SegReleaseBuffer(main_buffer);
            return false;
        }

        /* release the shared lock first to avoid dead lock */
        LockBuffer(map_buffer, BUFFER_LOCK_UNLOCK);
        LockBuffer(map_buffer, BUFFER_LOCK_EXCLUSIVE);

        head_blocknum = map_block->head_block[map_entry_id];
        /*
         * check the map entry again in case of someone else allocating the segment head between the lock
         * require/acquire above
         */
        if (head_blocknum == InvalidBlockNumber) {
            XLogAtomicOpStart();
            ExtentInversePointer iptr = {
                .flag = SPC_INVRSPTR_ASSEMBLE_FLAG(BUCKET_HEAD,
                                                   forknum * MAX_BUCKETMAPLEN + reln->smgr_rnode.node.bucketNode),
                .owner = reln->smgr_rnode.node.relNode};
            head_blocknum = normal_alloc_segment(reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                                 InvalidBlockNumber, iptr);
            map_block->head_block[map_entry_id] = head_blocknum;

            XLogAtomicOpRegisterBuffer(map_buffer, REGBUF_KEEP_DATA, SPCXLOG_BUCKET_ADD_BKTHEAD,
                                       XLOG_COMMIT_KEEP_BUFFER_STATE);
            XLogAtomicOpRegisterBufData((char *)&map_entry_id, sizeof(int));
            XLogAtomicOpRegisterBufData((char *)&head_blocknum, sizeof(BlockNumber));
            XLogAtomicOpCommit();
            SEGMENTTEST(FREE_EXTENT_INSERT_ONE_BUCKET, (errmsg("FREE_EXTENT_INSERT_ONE_BUCKET %s: "
                "all values insert into one bucket!\n", g_instance.attr.attr_common.PGXCNodeName)));
        }
    }

    SegmentDesc *seg_desc =
        (SegmentDesc *)MemoryContextAlloc(LocalSmgrStorageMemoryCxt(), sizeof(SegmentDesc));
    seg_desc->head_blocknum = head_blocknum;
    seg_desc->timeline = seg_get_drop_timeline();
    SegmentCheck(head_blocknum >= DF_MAP_GROUP_SIZE);
    reln->seg_desc[forknum] = seg_desc;

    SegUnlockReleaseBuffer(map_buffer);
    SegReleaseBuffer(main_buffer);

    return true;
}

static void bucket_unlink_one_bucket(SegSpace *spc, const RelFileNode &rnode)
{
    Buffer main_buffer;
    BktMainHead *main_head;
    bucket_load_main_head(spc, rnode.relNode, &main_buffer, &main_head);
    SegmentCheck(IsBucketMainHead(main_head));

    LockBuffer(main_buffer, BUFFER_LOCK_SHARE);
    for (int i = 0; i <= SEGMENT_MAX_FORKNUM; i++) {
        /* Locate the segment head for this fork */
        BlockNumber map_blocknum;
        int map_entry_id;
        bucket_get_mapentry(main_head, rnode.bucketNode, i, &map_blocknum, &map_entry_id);
        if (map_blocknum != 0) {
            Buffer map_buffer = ReadSegmentBuffer(spc, map_blocknum);
            LockBuffer(map_buffer, BUFFER_LOCK_EXCLUSIVE);
            BktHeadMapBlock *map_block = (BktHeadMapBlock *)PageGetContents(BufferGetPage(map_buffer));
            SegmentCheck(IsBucketMapBlock(map_block));
            BlockNumber head_blocknum = map_block->head_block[map_entry_id];

            if (head_blocknum != InvalidBlockNumber) {
                Buffer head_buffer = ReadSegmentBuffer(spc, head_blocknum);
                LockBuffer(head_buffer, BUFFER_LOCK_EXCLUSIVE);
                off_t offset = offsetof(BktHeadMapBlock, head_block) + sizeof(BlockNumber) * map_entry_id;
                free_one_segment(spc, i, head_buffer, head_blocknum, map_buffer, offset);

                SegUnlockReleaseBuffer(head_buffer);
            }
            SegUnlockReleaseBuffer(map_buffer);
        }
    }
    SegUnlockReleaseBuffer(main_buffer);
}

/*
 * main_buffer is pinned and locked before invocation
 */
static void bucket_free_one_mapblock(SegSpace *spc, Buffer main_buffer, uint32 map_id)
{
    BktMainHead *main_head = (BktMainHead *)PageGetContents(BufferGetPage(main_buffer));
    SegmentCheck(IsBucketMainHead(main_head));
    BlockNumber map_blocknum = main_head->bkt_map[map_id];

    main_head->bkt_map[map_id] = InvalidBlockNumber;
    XLogAtomicOpStart();
    spc_free_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, map_blocknum);

    XLogAtomicOpRegisterBuffer(main_buffer, REGBUF_KEEP_DATA, SPCXLOG_BUCKET_FREE_MAPBLOCK,
                               XLOG_COMMIT_KEEP_BUFFER_STATE);
    XLogAtomicOpRegisterBufData((char *)&map_id, sizeof(uint32));
    XLogAtomicOpCommit();
}

/*
 * Scan all map entry to find valid segment head and release them
 */
static void bucket_unlink_segment(SegSpace *spc, RelFileNode rnode, Buffer main_buffer)
{
    BlockNumber seghead = rnode.relNode;
    BktMainHead *main_head = (BktMainHead *)PageGetContents(BufferGetPage(main_buffer));
    bool freebucket = true;
    uint32 *redis_map = NULL;
    bool redis_committed = false;
    /*
     * For scale-in senario, there are 4 different conditions we should deal with:
     * 1 If redis xid is committed then,
     *   a. when we drop dest bucket relation, just drop all buckets.
     *   b. when we drop source bucket relation, nothing should be dropped.
     * 2 If redis xid is Aborted then,
     *   a. when we drop dest bucket relation, buckets in source bucket relation should not be dropped.
     *   b. when we drop source bucket relation, just drop all buckets.
     */
    if (TransactionIdIsValid(main_head->redis_info.redis_xid)) {
        /* We get here means the redis transaction is over */
        SegmentCheck(!TransactionIdIsInProgress(main_head->redis_info.redis_xid));
        /*
         * oldestXid cannot be updated when cluster is resizeing, so we assuming
         * that oldestXid bigger than redis_xid means cluster resizeing is over,
         * we can drop all the buckets safely.
         */
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->oldestXid, main_head->redis_info.redis_xid)) {
            if (TransactionIdDidCommit(main_head->redis_info.redis_xid)) {
                /* case 1b */
                if (main_head->redis_info.nwords == 0) {
                    freebucket = false;
                }
                redis_committed = true;
            } else if (main_head->redis_info.nwords != 0) {
                /* case 2a */
                SegmentCheck(main_head->redis_info.nwords == BktBitMaxMapCnt);
                redis_map = main_head->redis_info.words;
            }
        }
        ereport(LOG,
            (errmsg("rnode <%u %u %u %u>, xid [%lu, %lu, %lu], redis nwords %u, redis is [%s]",
                rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode,
                t_thrd.xact_cxt.ShmemVariableCache->oldestXid,
                main_head->redis_info.redis_xid,
                GetCurrentTransactionIdIfAny(),
                main_head->redis_info.nwords,
                redis_committed ? "committed" : "abort")));
    }

    LockBuffer(main_buffer, BUFFER_LOCK_EXCLUSIVE);

    for (uint32 i = 0; i < BktMapBlockNumber; i++) {
        BlockNumber map_blocknum = main_head->bkt_map[i];
        if (BlockNumberIsValid(map_blocknum)) {
            Buffer map_buffer = ReadSegmentBuffer(spc, map_blocknum);
            LockBuffer(map_buffer, BUFFER_LOCK_EXCLUSIVE);
            BktHeadMapBlock *map_block = (BktHeadMapBlock *)PageGetContents(BufferGetPage(map_buffer));
            SegmentCheck(IsBucketMapBlock(map_block));
            for (uint32 k = 0; freebucket && k < BktMapEntryNumberPerBlock; k++) {
                BlockNumber head_blocknum = map_block->head_block[k];
                if (head_blocknum != InvalidBlockNumber) {
                    if (redis_map && GET_BKT_MAP_BIT(redis_map, i * BktMapEntryNumberPerBlock + k)) {
                        continue;
                    }
                    /* Release the segment */
                    Buffer head_buffer = ReadSegmentBuffer(spc, head_blocknum);
                    LockBuffer(head_buffer, BUFFER_LOCK_EXCLUSIVE);
                    off_t offset = offsetof(BktHeadMapBlock, head_block) + sizeof(BlockNumber) * k;
                    ForkNumber forknum = (i * BktMapEntryNumberPerBlock + k) / MAX_BUCKETMAPLEN;
                    free_one_segment(spc, forknum, head_buffer, head_blocknum, map_buffer, offset);
                    SEGMENTTEST(FREE_EXTENT_DROP_BUCKETS, (errmsg("FREE_EXTENT_DROP_BUCKETS %s: "
                        "drop some buckets,remain buckets can drop !\n", g_instance.attr.attr_common.PGXCNodeName)));
                    SegUnlockReleaseBuffer(head_buffer);
                }
            }
            SegUnlockReleaseBuffer(map_buffer);
            bucket_free_one_mapblock(spc, main_buffer, i);
        }
    }

    XLogAtomicOpStart();
    spc_free_extent(spc, SEGMENT_HEAD_EXTENT_SIZE, MAIN_FORKNUM, seghead);
    XLogAtomicOpCommit();

    LockBuffer(main_buffer, BUFFER_LOCK_UNLOCK);
}

BlockNumber bucket_totalblocks(SMgrRelation reln, ForkNumber forknum, Buffer main_buffer)
{
    SMgrOpenSpace(reln);
    BktMainHead *main_head = (BktMainHead *)PageGetContents(BufferGetPage(main_buffer));
    SegSpace *spc = reln->seg_space;

    LockBuffer(main_buffer, BUFFER_LOCK_SHARE);

    BlockNumber result = 0;
    BlockNumber map_blocknum, last_map_blocknum = InvalidBlockNumber;
    int map_entry_id;
    Buffer map_buffer = InvalidBuffer;

    for (int i = 0; i < MAX_BUCKETMAPLEN; i++) {
        bucket_get_mapentry(main_head, i, forknum, &map_blocknum, &map_entry_id);
        if (map_blocknum != last_map_blocknum) {
            if (map_buffer != InvalidBuffer) {
                SegUnlockReleaseBuffer(map_buffer);
            }
            map_buffer = ReadSegmentBuffer(spc, map_blocknum);
            LockBuffer(map_buffer, BUFFER_LOCK_SHARE);
            last_map_blocknum = map_blocknum;
        }

        BktHeadMapBlock *map_block = (BktHeadMapBlock *)PageGetContents(BufferGetPage(map_buffer));
        SegmentCheck(IsBucketMapBlock(map_block));
        BlockNumber head_blocknum = map_block->head_block[map_entry_id];

        if (head_blocknum != InvalidBlockNumber) {
            Buffer head_buffer = ReadSegmentBuffer(spc, head_blocknum);
            SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetPage(head_buffer));

            LockBuffer(head_buffer, BUFFER_LOCK_SHARE);
            result += seg_head->nblocks;

            SegUnlockReleaseBuffer(head_buffer);
        }
    }

    if (map_buffer != InvalidBuffer) {
        SegUnlockReleaseBuffer(map_buffer);
    }
    LockBuffer(main_buffer, BUFFER_LOCK_UNLOCK);

    return result;
}

SegPageLocation seg_get_physical_location(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum)
{
    SMgrRelation reln;

    if (RecoveryInProgress()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("cannot get segment address translation during recovery")));
    }

    reln = smgropen(rnode, InvalidBackendId);
    Buffer buffer = read_head_buffer(reln, forknum, false);
    SegmentCheck(BufferIsValid(buffer));
    SegmentHead *head = (SegmentHead *)PageGetContents(BufferGetBlock(buffer));

    SegPageLocation loc = seg_logic_to_physic_mapping(reln, head, blocknum);

    SegReleaseBuffer(buffer);
    return loc;
}

/*
 * Allocate a segment
 *
 * This function is an atomic funciton, i.e., it will generate one xlog.
 */
BlockNumber seg_alloc_segment(Oid tablespace_id, Oid database_id, bool isbucket, BlockNumber preassigned_block)
{
    BlockNumber result;
    (void)GetCurrentTransactionId();
    if (isbucket) {
        /*
         * bucket_alloc_segment is divided into serveral atomic operations internal, so we do not wrap it here.
         */
        result = bucket_alloc_segment(tablespace_id, database_id, preassigned_block);
    } else {
        ExtentInversePointer iptr = {.flag = SPC_INVRSPTR_ASSEMBLE_FLAG(SEGMENT_HEAD, 0), .owner = InvalidBlockNumber};
        XLogAtomicOpStart();
        result = normal_alloc_segment(tablespace_id, database_id, preassigned_block, iptr, true);
        SEGMENTTEST(FREE_EXTENT_CREATE_SEGMENT_UNCOMMIT, (errmsg("FREE_EXTENT_CREATE_SEGMENT_UNCOMMIT %s: alloc segment success, "
            "transation uncommit can drop segment!\n", g_instance.attr.attr_common.PGXCNodeName)));
        XLogAtomicOpCommit();
    }
    return result;
}

/*
 * Open a segment for the given forknum
 *
 * args:
 *  - create: whether create a new segment if it does not exist.
 *
 * If the segment exists or it is created successfully, return true; otherwise return false.
 */
static bool open_segment(SMgrRelation reln, ForkNumber forknum, bool create, XLogRecPtr lsn)
{
    if (!IsNormalForknum(forknum)) {
        SegmentCheck(!create);
        return false;
    }

    if (reln->seg_desc[forknum]) {
        if (forknum != MAIN_FORKNUM && !CurrentThreadIsWorker() &&
            reln->seg_desc[forknum]->timeline != seg_get_drop_timeline()) {
            /*
             * It's possible that the current smgr cache is invalid. We should close it and reopen.
             * Worker threads can skip this step, because SMgrRelation objects are invalidated by messages.
             */
            pfree(reln->seg_desc[forknum]);
            reln->seg_desc[forknum] = NULL;
        } else {
            return true;
        }
    }

    ASSERT_NORMAL_FORK(forknum);
    SMgrOpenSpace(reln);
    SegmentCheck(reln->seg_space != NULL);
    if (IsBucketSMgrRelation(reln)) {
        return bucket_open_segment(reln, forknum, create, lsn);
    } else {
        return normal_open_segment(reln, forknum, create);
    }
}

/*
 * Read segment head buffer
 *
 * Return InvalidBuffer if the segment does not exist, and create flag is false
 */
static Buffer read_head_buffer(SMgrRelation reln, ForkNumber forknum, bool create)
{
    ASSERT_NORMAL_FORK(forknum);

    bool exist = open_segment(reln, forknum, create);
    if (!exist) {
        return InvalidBuffer;
    }
    SegmentCheck(reln->seg_desc[forknum] != NULL);
    Buffer buffer = ReadSegmentBuffer(reln->seg_space, reln->seg_desc[forknum]->head_blocknum);

#ifdef USE_ASSERT_CHECKING
    SegmentHead *head = (SegmentHead *)PageGetContents(BufferGetPage(buffer));
    uint64 header_magic = head->magic;
    SegmentCheck((header_magic == SEGMENT_HEAD_MAGIC || header_magic == BUCKET_SEGMENT_MAGIC));
#endif

    return buffer;
}

static Buffer read_head_buffer_redo(SMgrRelation reln, ForkNumber forknum, XLogRecPtr lsn)
{
    ASSERT_NORMAL_FORK(forknum);

    bool exist = open_segment(reln, forknum, false, lsn);
    if (!exist) {
        return InvalidBuffer;
    }
    SegmentCheck(reln->seg_desc[forknum] != NULL);
    Buffer buffer = ReadSegmentBuffer(reln->seg_space, reln->seg_desc[forknum]->head_blocknum);

    return buffer;
}


static void seg_unlink_segment(const RelFileNode &rnode)
{
    seg_update_timeline();

    SegSpace *spc = spc_open(rnode.spcNode, rnode.dbNode, false);
    SegmentCheck(spc != NULL);

    if (!IsBucketFileNode(rnode)) {
        Buffer buffer = ReadSegmentBuffer(spc, rnode.relNode);
        SegmentHead *head = (SegmentHead *)PageGetContents(BufferGetBlock(buffer));

        if (head->magic == SEGMENT_HEAD_MAGIC) {
            // normal table
            SEGMENTTEST(FREE_SEGMENT_DROP_SEGMENT, (errmsg("FREE_SEGMENT_DROP_SEGMENT %s: "
                "drop segment tb success!\n", g_instance.attr.attr_common.PGXCNodeName)));
            normal_unlink_segment(spc, rnode, buffer);
        } else if (head->magic == BUCKET_SEGMENT_MAGIC) {
            // Delete all buckets in the table
            bucket_unlink_segment(spc, rnode, buffer);
        } else {
            SegmentCheck(false);
        }

        SegReleaseBuffer(buffer);
    } else {
        // Delete one bucket
        bucket_unlink_one_bucket(spc, rnode);
    }
}

static inline void LOG_SMGR_API(RelFileNodeBackend smgr_rnode, ForkNumber forknum, BlockNumber blocknum,
                                const char *func_name, int elevel = DEBUG2)
{
    RelFileNode rnode = smgr_rnode.node;
    ereport(DEBUG2, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("Segment-page smgr api: %s is invoked, SMgrRelation: <%u, %u, %u, %u> %d %u", func_name,
                            rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode, forknum, blocknum)));
}

/*
 * API functions for smgr
 */
void seg_init()
{
    // do nothing
}

void seg_shutdown()
{
    // do nothing
}

void seg_close(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum)
{
    SegmentCheck(blockNum == InvalidBlockNumber);
    LOG_SMGR_API(reln->smgr_rnode, forknum, InvalidBlockNumber, "seg_close");

    if (reln->seg_desc && reln->seg_desc[forknum] != NULL) {
        /* release memory */
        pfree(reln->seg_desc[forknum]);
        reln->seg_desc[forknum] = NULL;
    }
}

void seg_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, InvalidBlockNumber, "seg_create");
    bool res = open_segment(reln, forknum, true);
    SegmentCheck(res == true);
    (void)res; // keep compiler silent
}

bool seg_exists(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, InvalidBlockNumber, "seg_exists");
    return open_segment(reln, forknum, false);
}

void seg_unlink(const RelFileNodeBackend &rnode, ForkNumber forknum, bool isRedo, BlockNumber blockNum)
{
    if (isRedo) {
        return;
    }
    LOG_SMGR_API(rnode, forknum, InvalidBlockNumber, "seg_unlink", LOG);
    SegmentCheck(forknum == InvalidForkNumber);
    seg_unlink_segment(rnode.node);
}

struct ExtendStat {
  private:
    instr_time start_time;
    instr_time end_time;
    Oid _spc;
    Oid _db;
    Oid _file;

  public:
    void set_file(Oid spc, Oid db, Oid file)
    {
        _spc = spc;
        _db = db;
        _file = file;
    }

    void start()
    {
        (void)INSTR_TIME_SET_CURRENT(start_time);
    }

    void end()
    {
        (void)INSTR_TIME_SET_CURRENT(end_time);
        INSTR_TIME_SUBTRACT(end_time, start_time);
        PgStat_Counter time_diff = (PgStat_Counter)INSTR_TIME_GET_MICROSEC(end_time);

        PgStat_MsgFile msg;
        errno_t rc = memset_s(&msg, sizeof(msg), 0, sizeof(msg));
        securec_check(rc, "", "");

        msg.dbid = _db;
        msg.spcid = _spc;
        msg.fn = _file;
        msg.rw = 'w';
        msg.cnt = 1;
        msg.blks = 1;
        msg.tim = time_diff;
        msg.lsttim = time_diff;
        msg.mintim = time_diff;
        msg.maxtim = time_diff;
        reportFileStat(&msg);
    }
};

/*
 * Allocate extents to a segment until there are enough space to hold logic block number "blocknum"
 */
void seg_extend_internal(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
    Buffer buffer = read_head_buffer(reln, forknum, true);
    SegmentCheck(BufferIsValid(buffer));
    SegmentHead *head = (SegmentHead *)PageGetContents(BufferGetBlock(buffer));
    SegmentCheck(IsNormalSegmentHead(head));

    /*
     * We do not need to lock segment head buffer here, because there is always only-one
     * thread calling smgrextend due to LockRelationForExtension
     */
    while (head->total_blocks <= blocknum) {
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        seg_extend_segment(reln->seg_space, forknum, buffer, reln->seg_desc[forknum]->head_blocknum);
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    }

    SegReleaseBuffer(buffer);
}

void seg_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, blocknum, "seg_extend");

    Buffer buf = BlockGetBuffer(buffer);
    /* All segment pages are extended through buffer, to set LSN correctly and use double-write */
    SegmentCheck(BufferIsValid(buf));

#ifdef USE_ASSERT_CHECKING
    bool all_zeroes = true;
    size_t *pagebytes = (size_t *)buffer;
    for (int i = 0; i < (int)(BLCKSZ / sizeof(size_t)); i++) {
        if (pagebytes[i] != 0) {
            all_zeroes = false;
            break;
        }
    }
    SegmentCheck(all_zeroes);
    SegmentCheck(skipFsync == false);
#endif

    BufferDesc *buf_desc = BufferGetBufferDescriptor(buf);
    SegmentCheck(!LWLockHeldByMe(buf_desc->content_lock));
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    ExtendStat ext_stat;
    RelFileNode rnode = reln->smgr_rnode.node;
    // we use "0 - relfilenode" as file node to represent extend events.
    ext_stat.set_file(rnode.spcNode, rnode.dbNode, 0 - rnode.relNode);
    ext_stat.start();

    /* Extend enough space */
    seg_extend_internal(reln, forknum, blocknum);

    Buffer seg_buffer = read_head_buffer(reln, forknum, true);
    SegmentCheck(BufferIsValid(seg_buffer));
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetBlock(seg_buffer));
    SegmentCheck(IsNormalSegmentHead(seg_head));

    LockSegmentHeadPartition(reln->seg_space->spcNode, reln->seg_space->dbNode, reln->seg_desc[forknum]->head_blocknum,
                             LW_SHARED);

    SegPageLocation loc = seg_logic_to_physic_mapping(reln, seg_head, blocknum);

    LockBuffer(seg_buffer, BUFFER_LOCK_EXCLUSIVE);

    ereport(DEBUG5,
            (errmodule(MOD_SEGMENT_PAGE),
             errmsg("Relfilenode %u, segment %u extend, old nblocks %u, total_blocks %u, target %u", rnode.relNode,
                    reln->seg_desc[forknum]->head_blocknum, seg_head->nblocks, seg_head->total_blocks, blocknum)));

    /* Add physical location for XLog */
    buf_desc->seg_fileno = (uint8)EXTENT_SIZE_TO_TYPE(loc.extent_size);
    buf_desc->seg_blockno = loc.blocknum;

    if (seg_head->nblocks <= blocknum) {
        XLogDataSegmentExtend xlog_data;
        xlog_data.old_nblocks = seg_head->nblocks;
        xlog_data.new_nblocks = blocknum + 1;
        xlog_data.ext_size = loc.extent_size;
        xlog_data.blocknum = loc.blocknum;
        xlog_data.main_fork_head = reln->smgr_rnode.node.relNode;
        xlog_data.reset_zero = skipFsync ? 0 : 1;
        xlog_data.forknum = forknum;

        /* XLog issue */
        START_CRIT_SECTION();
        seg_head->nblocks = blocknum + 1;
        SegMarkBufferDirty(seg_buffer);
        MarkBufferDirty(buf);

        XLogBeginInsert();
        XLogRegisterBuffer(0, seg_buffer, REGBUF_KEEP_DATA);
        XLogRegisterBufData(0, (char *)&xlog_data, sizeof(xlog_data));
        XLogRegisterBuffer(1, buf, REGBUF_WILL_INIT);
        XLogRecPtr xlog_rec = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_SEGMENT_EXTEND, SegmentBktId);
        END_CRIT_SECTION();

        PageSetLSN(BufferGetPage(seg_buffer), xlog_rec);
        PageSetLSN(buffer, xlog_rec);
    }

    SegUnlockReleaseBuffer(seg_buffer);
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    UnlockSegmentHeadPartition(reln->seg_space->spcNode, reln->seg_space->dbNode,
                               reln->seg_desc[forknum]->head_blocknum);

    ext_stat.end();
}

void seg_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, blocknum, "seg_prefetch");
    /* Not support prefetch yet, just return */
}

SMGR_READ_STATUS seg_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, blocknum, "seg_read");

    Buffer seg_buffer = read_head_buffer(reln, forknum, false);
    SegmentCheck(BufferIsValid(seg_buffer));
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetBlock(seg_buffer));
    SegmentCheck(IsNormalSegmentHead(seg_head));

    if (seg_head->nblocks <= blocknum) {
        SegReleaseBuffer(seg_buffer);
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("seg_read blocknum exceeds segment size"),
                        errdetail("segment %s, head: %u, read block %u, but nblocks is %u",
                                  relpathperm(reln->smgr_rnode.node, forknum), reln->seg_desc[forknum]->head_blocknum,
                                  blocknum, seg_head->nblocks)));
    }

    LockSegmentHeadPartition(reln->seg_space->spcNode, reln->seg_space->dbNode, reln->seg_desc[forknum]->head_blocknum,
                             LW_SHARED);

    SegPageLocation loc = seg_logic_to_physic_mapping(reln, seg_head, blocknum);
    SegmentCheck(loc.blocknum != InvalidBlockNumber);

    SegSpace *spc = reln->seg_space;
    spc_read_block(spc, EXTENT_GROUP_RNODE(spc, loc.extent_size), forknum, buffer, loc.blocknum);

    UnlockSegmentHeadPartition(reln->seg_space->spcNode, reln->seg_space->dbNode,
                               reln->seg_desc[forknum]->head_blocknum);

    /* extent_size == 1 means segment metadata, which should not be used as heap data */
    SegmentCheck(loc.extent_size != 1);

    /* Set physical location in buffer descriptor, which is used for XLog */
    Buffer buf = BlockGetBuffer(buffer);
    if (BufferIsValid(buf)) {
        BufferDesc *buf_desc = BufferGetBufferDescriptor(buf);
        buf_desc->seg_fileno = (uint8)EXTENT_SIZE_TO_TYPE(loc.extent_size);
        buf_desc->seg_blockno = loc.blocknum;
    }

    SegReleaseBuffer(seg_buffer);

    if (PageIsVerified((Page)buffer, loc.blocknum)) {
        return SMGR_RD_OK;
    } else {
        return SMGR_RD_CRC_ERROR;
    }
}

void seg_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, blocknum, "seg_write");

    Buffer seg_buffer = read_head_buffer(reln, forknum, true);
    SegmentCheck(BufferIsValid(seg_buffer));
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetBlock(seg_buffer));
    SegmentCheck(IsNormalSegmentHead(seg_head));

    if (seg_head->nblocks <= blocknum) {
        SegReleaseBuffer(seg_buffer);
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("seg_write blocknum exceeds segment size"),
                        errdetail("segment %s, head: %u, read block %u, but nblocks is %u",
                                  relpathperm(reln->smgr_rnode.node, forknum), reln->seg_desc[forknum]->head_blocknum,
                                  blocknum, seg_head->nblocks)));
    }

    LockSegmentHeadPartition(reln->seg_space->spcNode, reln->seg_space->dbNode, reln->seg_desc[forknum]->head_blocknum,
                             LW_SHARED);

    SegPageLocation loc = seg_logic_to_physic_mapping(reln, seg_head, blocknum);
    SegmentCheck(loc.blocknum != InvalidBlockNumber);

    UnlockSegmentHeadPartition(reln->seg_space->spcNode, reln->seg_space->dbNode,
                               reln->seg_desc[forknum]->head_blocknum);

    // TODO: remove PageSetChecksumInplace invocation outside SMGR.
    PageSetChecksumInplace((Page)buffer, loc.blocknum);
    SegSpace *spc = reln->seg_space;
    spc_write_block(spc, EXTENT_GROUP_RNODE(spc, loc.extent_size), forknum, buffer, loc.blocknum);

    SegReleaseBuffer(seg_buffer);
}

void seg_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, InvalidBlockNumber, "seg_writeback");

    RelFileNode rNode = reln->smgr_rnode.node;
    if (IsSegmentPhysicalRelNode(rNode)) {
        SMgrOpenSpace(reln);
        if (reln->seg_space == NULL) {
            ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("when write back, segment space [%u, %u] doesn't exist.", reln->smgr_rnode.node.spcNode,
                           reln->smgr_rnode.node.dbNode)));
            return;
        }
        spc_writeback(reln->seg_space, EXTENT_TYPE_TO_SIZE(rNode.relNode), forknum, blocknum, nblocks);
    } else {
        /*
         * Logical writes are continues in each extent.
         * XXX: Continues extents can be merged to reduece system call.
         */
        Buffer seg_buffer = read_head_buffer(reln, forknum, true);
        SegmentCheck(BufferIsValid(seg_buffer));
        SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetBlock(seg_buffer));
        SegmentCheck(IsNormalSegmentHead(seg_head));
        SegmentCheck(blocknum + nblocks < seg_head->nblocks);

        /* Get the start extent id */
        SegPageLocation loc1 = seg_logic_to_physic_mapping(reln, seg_head, blocknum);
        SegmentCheck(loc1.blocknum != InvalidBlockNumber);
        uint32 curr_ext_id = loc1.extent_id;

        while (nblocks > 0) {
            BlockNumber nflush = nblocks;

            BlockNumber curr_ext_start = seg_extent_location(reln->seg_space, seg_head, curr_ext_id);
            BlockNumber ext_size = ExtentSizeByCount(curr_ext_id);

            if (nblocks > ext_size) {
                nflush = ext_size;
            }

            spc_writeback(reln->seg_space, ext_size, forknum, curr_ext_start, nflush);
            nblocks -= nflush;

            if (nblocks > 0) {
                curr_ext_id++;
            }
        }
        SegReleaseBuffer(seg_buffer);
    }
}

BlockNumber seg_nblocks(SMgrRelation reln, ForkNumber forknum)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, InvalidBlockNumber, "seg_nblocks");

    ASSERT_NORMAL_FORK(forknum);

    bool seg_exist = open_segment(reln, forknum, false);
    if (!seg_exist) {
        return 0;
    }

    Buffer seg_buffer = read_head_buffer(reln, forknum, false);
    SegmentCheck(BufferIsValid(seg_buffer));
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetBlock(seg_buffer));
    SegmentCheck(IsNormalSegmentHead(seg_head));

    LockBuffer(seg_buffer, BUFFER_LOCK_SHARE);
    BlockNumber nblocks = seg_head->nblocks;
    SegUnlockReleaseBuffer(seg_buffer);

    return nblocks;
}

void seg_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
    SegmentCheck(0);
}

void seg_move_buckets(const RelFileNodeBackend &dest, const RelFileNodeBackend &src, List *bucketList)
{
    LOG_SMGR_API(dest, InvalidForkNumber, InvalidBlockNumber, "seg_move_buckets");
    uint32 i,j;
    uint32 nentry;
    ListCell *cell = NULL;
    BktMainHead *shead, *dhead;
    Buffer dbuffer, sbuffer;
    xl_seg_bktentry_tag_t mapentry[BktMapEntryNumberPerBlock];
    TransactionId redis_xid = GetCurrentTransactionId();

    /* make sure they are sharing the same segspc */
    SegmentCheck(dest.node.spcNode == src.node.spcNode && dest.node.dbNode == src.node.dbNode);
    SegSpace *spc = spc_open(dest.node.spcNode, dest.node.dbNode, false);

    bucket_load_main_head(spc, dest.node.relNode, &dbuffer, &dhead);
    bucket_load_main_head(spc, src.node.relNode, &sbuffer, &shead);

    LockBuffer(dbuffer, BUFFER_LOCK_EXCLUSIVE);
    LockBuffer(sbuffer, BUFFER_LOCK_EXCLUSIVE);
    XLogAtomicOpStart();

    /* Add redis info for both source and dest bucket segment header */
    dhead->redis_info.redis_xid  = redis_xid;
    shead->redis_info.redis_xid  = redis_xid;
    dhead->redis_info.nwords = BktBitMaxMapCnt;
    shead->redis_info.nwords = 0;
    for (i = 0; i < BktBitMaxMapCnt; i++) {
        dhead->redis_info.words[i] = 0;
        shead->redis_info.words[i] = 0;
    }

    /* start to do bucket segment move */
    for (i = 0; i < BktMapBlockNumber; i++) {
        SegmentCheck(dhead->bkt_map[i] != InvalidBlockNumber);
        SegmentCheck(shead->bkt_map[i] != InvalidBlockNumber);
        nentry = 0;
        Buffer smapbuffer = ReadSegmentBuffer(spc, shead->bkt_map[i]);
        Buffer dmapbuffer = ReadSegmentBuffer(spc, dhead->bkt_map[i]);
        BktHeadMapBlock *smapblock = (BktHeadMapBlock *)PageGetContents(BufferGetPage(smapbuffer));
        BktHeadMapBlock *dmapblock = (BktHeadMapBlock *)PageGetContents(BufferGetPage(dmapbuffer));
        LockBuffer(smapbuffer, BUFFER_LOCK_EXCLUSIVE);
        LockBuffer(dmapbuffer, BUFFER_LOCK_EXCLUSIVE);

        for (j = 0; j < BktMapEntryNumberPerBlock; j++) {
            int buckets = i * BktMapEntryNumberPerBlock + j;
            int2 bucketId = buckets % MAX_BUCKETMAPLEN;
            if (bucketId == 0) {
                cell = list_head(bucketList);
            }
            /* check if bucketid in move bucket list */
            if (!PointerIsValid(cell) || ((int2)lfirst_oid(cell) != bucketId)) {
                continue;
            }
            cell = lnext(cell);
            if (smapblock->head_block[j] != InvalidBlockNumber) {
                SET_BKT_MAP_BIT(dhead->redis_info.words, bucketId);
                mapentry[nentry].bktentry_id = j;
                mapentry[nentry++].bktentry_header = smapblock->head_block[j];
                /* Copy bucket entry from source to dest */
                dmapblock->head_block[j] = smapblock->head_block[j];
            } else if (buckets < MAX_BUCKETMAPLEN) {
                SegmentCheck(0);
            }
        }

        if (nentry != 0) {
            /* XLOG stuff */
            log_move_segment_buckets(mapentry, nentry, dmapbuffer);
        } else {
            /* release lock by ourself */
            SegUnlockReleaseBuffer(dmapbuffer);
        }
        SegUnlockReleaseBuffer(smapbuffer);
    }

    log_move_segment_redisinfo(&dhead->redis_info, &shead->redis_info, dbuffer, sbuffer);
    XLogAtomicOpCommit();
}

XLogRecPtr seg_get_headlsn(SegSpace *spc, BlockNumber blockNum, bool isbucket)
{
    Buffer buffer = ReadSegmentBuffer(spc, blockNum);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    SegmentHead *head = (SegmentHead *)PageGetContents(BufferGetPage(buffer));
    uint64 magic = isbucket ? BUCKET_SEGMENT_MAGIC : SEGMENT_HEAD_MAGIC;
    XLogRecPtr lsn = InvalidXLogRecPtr;

    if (PageIsSegmentVersion(BufferGetPage(buffer)) && head->magic == magic) {
        lsn = head->lsn;
        SegmentCheck(XLogRecPtrIsValid(lsn));
    }

    SegUnlockReleaseBuffer(buffer);
    return lsn;
}

void seg_immedsync(SMgrRelation reln, ForkNumber forknum)
{
}

void seg_pre_ckpt(void)
{
}

void seg_sync(void)
{
}

void seg_post_ckpt(void)
{
}

void seg_async_read(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn)
{
}

void seg_async_write(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn)
{
}

BlockNumber seg_fork_totalblocks(SMgrRelation reln, ForkNumber forknum)
{
    bool seg_exist = open_segment(reln, forknum, false);

    if (!seg_exist) {
        return 0;
    }

    Buffer seg_buffer = read_head_buffer(reln, forknum, false);
    SegmentCheck(BufferIsValid(seg_buffer));
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetBlock(seg_buffer));

    LockBuffer(seg_buffer, BUFFER_LOCK_SHARE);
    BlockNumber nblocks = seg_head->nblocks;
    SegUnlockReleaseBuffer(seg_buffer);

    return nblocks;
}

BlockNumber seg_totalblocks(SMgrRelation reln, ForkNumber forknum)
{
    LOG_SMGR_API(reln->smgr_rnode, forknum, InvalidBlockNumber, "seg_totalblocks");

    if (forknum > SEGMENT_MAX_FORKNUM) {
        return 0;
    }
    ASSERT_NORMAL_FORK(forknum);

    Buffer seg_buffer = read_head_buffer(reln, MAIN_FORKNUM, false);
    SegmentCheck(BufferIsValid(seg_buffer));
    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetBlock(seg_buffer));

    BlockNumber res = 0;
    if (seg_head->magic == BUCKET_SEGMENT_MAGIC) {
        res = bucket_totalblocks(reln, forknum, seg_buffer);
    } else {
        res = seg_fork_totalblocks(reln, forknum);
    }

    SegReleaseBuffer(seg_buffer);
    return res;
}

bool seg_fork_exists(SegSpace *spc, SMgrRelation reln, ForkNumber forknum, const XLogPhyBlock *pblk)
{
    ASSERT_NORMAL_FORK(forknum);

    RelFileNode rnode = reln->smgr_rnode.node;
    BlockNumber lastblock = spc_size(spc, pblk->relNode, forknum);

    if (pblk->block >= lastblock) {
        return false;
    }

    /* Check the data file that the head block belongs to */
    if (rnode.relNode >= spc_size(spc, SEGMENT_HEAD_EXTENT_TYPE, MAIN_FORKNUM)) {
        return false;
    }

    /* check if  main relation is still here */
    XLogRecPtr seglsn = seg_get_headlsn(spc, rnode.relNode, IsBucketFileNode(rnode));

    if (XLogRecPtrIsInvalid(seglsn) || XLByteLE(pblk->lsn, seglsn)) {
        /* segment header is reused */
        return false;
    }

    /* check if relation or bucket's fork is still here */
    Buffer seg_buffer = read_head_buffer_redo(reln, forknum, pblk->lsn);
    if (!BufferIsValid(seg_buffer)) {
        /* bucket or fork is dropped */
        return false;
    }

    SegmentHead *seg_head = (SegmentHead *)PageGetContents(BufferGetBlock(seg_buffer));
    LockBuffer(seg_buffer, BUFFER_LOCK_SHARE);
    if (!PageIsSegmentVersion(BufferGetBlock(seg_buffer)) || seg_head->magic != SEGMENT_HEAD_MAGIC) {
        /* segment header reused */
        SegUnlockReleaseBuffer(seg_buffer);
        return false;
    }

    bool ret = false;
    /* segment is exists check if it is resued*/
    SegmentCheck(XLogRecPtrIsValid(seg_head->lsn));
    if (XLByteLT(seg_head->lsn, pblk->lsn)) {
        ret = (seg_head->total_blocks != 0);
    }

    SegUnlockReleaseBuffer(seg_buffer);
    return ret;
}

/* APIs for others */

void seg_preextend(RelFileNode &rNode, ForkNumber forkNum, BlockNumber blkno)
{
    SMgrRelation reln = smgropen(rNode, InvalidBackendId);
    seg_extend_internal(reln, forkNum, blkno);
}

void seg_physical_read(SegSpace *spc, RelFileNode &rNode, ForkNumber forknum, BlockNumber blocknum, char *buffer)
{
    SegmentCheck(IsSegmentPhysicalRelNode(rNode));
    SegmentCheck(spc != NULL);
    spc_read_block(spc, rNode, forknum, buffer, blocknum);
}

void seg_physical_write(SegSpace *spc, RelFileNode &rNode, ForkNumber forknum, BlockNumber blocknum, const char *buffer,
                        bool skipFsync)
{
    SegmentCheck(IsSegmentPhysicalRelNode(rNode));
    SegmentCheck(spc != NULL);

    spc_write_block(spc, rNode, forknum, buffer, blocknum);
}
