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
 * segxlog.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segment/segxlog.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog_basic.h"
#include "access/xlogproc.h"
#include "access/xlogutils.h"
#include "access/multi_redo_api.h"
#include "access/double_write.h"
#include "catalog/storage_xlog.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "storage/smgr/segment.h"

/*
 * Truncate segment size
 */
static void redo_truncate(XLogReaderState *record)
{
    RedoBufferInfo buffer_info;
    XLogRedoAction redo_action = XLogReadBufferForRedo(record, 0, &buffer_info);
    if (redo_action == BLK_NEEDS_REDO) {
        BlockNumber nblocks = *(BlockNumber *)XLogRecGetBlockData(record, 0, NULL);

        Page page = buffer_info.pageinfo.page;
        SegmentHead *seg_head = (SegmentHead *)PageGetContents(page);
        seg_head->nblocks = nblocks;
        PageSetLSN(page, buffer_info.lsn);

        SegMarkBufferDirty(buffer_info.buf);
    }

    if (BufferIsValid(buffer_info.buf)) {
        SegUnlockReleaseBuffer(buffer_info.buf);
    }
}

/*
 * Move a list of buckets.
 */
static void redo_move_buckets(Buffer buffer, const char *data)
{
    xl_seg_bktentry_tag_t *bktentry;
    uint32 nentry;

    nentry = *(uint32 *)data;
    bktentry = (xl_seg_bktentry_tag_t *)(data + sizeof(uint32));

    BktHeadMapBlock *mapblock = (BktHeadMapBlock *)PageGetContents(BufferGetPage(buffer));
    for (uint32 i = 0; i < nentry; i++) {
        uint32 mapentry_id = bktentry[i].bktentry_id;
        mapblock->head_block[mapentry_id] = bktentry[i].bktentry_header;
    }
}

/*
 * Record redis info for bucket relation.
 */
static void redo_bucket_add_redisinfo(Buffer buffer, const char *data)
{
    SegRedisInfo *redis_info  = (SegRedisInfo *)data;

    if (redis_info->nwords  == 0) {
        BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
        Relation reln = CreateFakeRelcacheEntry(buf_desc->tag.rnode);

        RelationOpenSmgr(reln);
        flush_all_buffers(reln, InvalidOid);
        FreeFakeRelcacheEntry(reln);
    }

    BktMainHead *head = (BktMainHead *)PageGetContents(BufferGetPage(buffer));
    head->redis_info  = *redis_info;
}

static void redo_unset_bitmap(Buffer buffer, const char *data)
{
    uint16 bitid = *(uint16 *)data;
    data += sizeof(uint16);

    df_map_page_t *log_map_page = (df_map_page_t *)data;
    df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetPage(buffer));

    if (map_page->first_page != log_map_page->first_page) {
        ereport(PANIC,
                (errmsg("MapPage's first_page is not consistent, it's %u on disk but should be %u according to xlog",
                        map_page->first_page, log_map_page->first_page)));
    }
    
    if (DF_MAP_FREE(map_page->bitmap, bitid)) {
        ereport(PANIC, (errmsg("Try to unset bitmap which is free: %u", bitid)));
    }
    map_page->dirty_last = log_map_page->dirty_last;
    map_page->free_begin = log_map_page->free_begin;
    map_page->free_bits = log_map_page->free_bits;
    DF_MAP_UNSET(map_page->bitmap, bitid);
}

static void redo_set_bitmap(RedoBufferTag buftag, Buffer buffer, const char *data)
{
    uint16 bitid = *(uint16 *)data;
    data += sizeof(uint16);

    df_map_page_t *log_map_page = (df_map_page_t *)data;
    df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetPage(buffer));

    if (map_page->first_page != log_map_page->first_page) {
        ereport(PANIC,
                (errmsg("MapPage's first_page is not consistent, it's %u on disk but should be %u according to xlog",
                        map_page->first_page, log_map_page->first_page),
                 errhint("segment-page may have bug")));
    }
    if (DF_MAP_NOT_FREE(map_page->bitmap, bitid)) {
        ereport(PANIC,
                (errmsg("Try to set bitmap which is not free: %u", bitid), errhint("segment-page may have bug")));
    }
    map_page->dirty_last = log_map_page->dirty_last;
    map_page->free_begin = log_map_page->free_begin;
    map_page->free_bits = log_map_page->free_bits;
    DF_MAP_SET(map_page->bitmap, bitid);

    /* Extend data file if necessary. First get the extent start */
    BlockNumber blkno = map_page->first_page + EXTENT_TYPE_TO_SIZE(buftag.rnode.relNode) * bitid;
    /* Then plus one extent length */
    BlockNumber target = blkno + EXTENT_TYPE_TO_SIZE(buftag.rnode.relNode);
    SegSpace *spc = spc_open(buftag.rnode.spcNode, buftag.rnode.dbNode, false);
    SegmentCheck(spc != NULL);
    spc_extend_file(spc, buftag.rnode.relNode, buftag.forknum, target);
}

static void redo_maphead_allocated_extents(Buffer buffer, const char *data)
{
    XLogDataSpaceAllocateExtent *xlog_data = (XLogDataSpaceAllocateExtent *)data;
    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetPage(buffer));

    map_head->allocated_extents = xlog_data->allocated_extents;
    map_head->free_group = xlog_data->free_group;
    map_head->groups[xlog_data->free_group].free_page  = xlog_data->free_page;
    map_head->high_water_mark = xlog_data->hwm;
}

static void redo_init_seghead(Buffer buffer, const char *data)
{
    BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
    SegmentCheck(buf_desc->tag.rnode.relNode == EXTENT_SIZE_TO_TYPE(SEGMENT_HEAD_EXTENT_SIZE));
    XLogRecPtr lsn = *(XLogRecPtr *)(data);
    eg_init_segment_head_buffer_content(buffer, buf_desc->tag.blockNum, lsn);
}

static void redo_update_seghead(Buffer buffer, const char *data)
{
    XLogDataUpdateSegmentHead *xlog_data = (XLogDataUpdateSegmentHead *)data;

    SegmentHead *head = (SegmentHead *)PageGetContents(BufferGetPage(buffer));

    if (xlog_data->nblocks != head->nblocks) {
        ereport(PANIC,
                (errmsg("redo update seghead, but target head's nblocks is %u, but should be %u according to xlog",
                        head->nblocks, xlog_data->nblocks),
                 errhint("segment-page may have bug")));
    }

    if (xlog_data->level0_slot >= 0) {
        head->level0_slots[xlog_data->level0_slot] = xlog_data->level0_value;
    }
    if (xlog_data->level1_slot >= 0) {
        head->level1_slots[xlog_data->level1_slot] = xlog_data->level1_value;
    }
    head->nextents = xlog_data->nextents;
    head->total_blocks = xlog_data->total_blocks;
    /* If all extents is freed, flush segment header to disk */
    if (head->total_blocks == 0) {
        SegmentCheck(head->nextents == 0);
        FlushOneSegmentBuffer(buffer);
    }
}

static void redo_new_level0_page(Buffer buffer, const char *data)
{
    BlockNumber first_extent = *(BlockNumber *)data;
    Page level0_page = BufferGetPage(buffer);

    SegPageInit(level0_page, BLCKSZ);
    PageHeader header = (PageHeader)level0_page;
    header->pd_lower += sizeof(BMTLevel0Page);

    BMTLevel0Page *bmt_level0_page = (BMTLevel0Page *)PageGetContents(level0_page);
    bmt_level0_page->slots[0] = first_extent;
    bmt_level0_page->magic = BMTLEVEL0_MAGIC;
}

static void redo_level0_page_add_extent(Buffer buffer, const char *data)
{
    XLogDataSetLevel0Page *xlog_data = (XLogDataSetLevel0Page *)data;

    BMTLevel0Page *bmt_level0_page = (BMTLevel0Page *)PageGetContents(BufferGetPage(buffer));
    bmt_level0_page->slots[xlog_data->slot] = xlog_data->extent;
}

static void redo_seghead_add_fork_segment(Buffer buffer, const char *data)
{
    int forknum = *(int *)data;
    data += sizeof(int);
    BlockNumber forkhead = *(BlockNumber *)data;

    SegmentHead *seghead = (SegmentHead *)PageGetContents(BufferGetPage(buffer));
    seghead->fork_head[forknum] = forkhead;
}

static void redo_unlink_seghead_ptr(Buffer buffer, const char *data)
{
    off_t offset = *(off_t *)data;

    BlockNumber *owner_pointer = (BlockNumber *)((char *)PageGetContents(BufferGetPage(buffer)) + offset);
    *owner_pointer = InvalidBlockNumber;
}

static void redo_init_bucket_main_head(Buffer buffer, const char *data)
{
    Page main_head_page = BufferGetPage(buffer);
    SegPageInit(main_head_page, BLCKSZ);
    ((PageHeader)main_head_page)->pd_lower += sizeof(BktMainHead);

    BktMainHead *main_head = (BktMainHead *)PageGetContents(main_head_page);
    main_head->magic = BUCKET_SEGMENT_MAGIC;
    main_head->lsn = *(XLogRecPtr *)data;
    main_head->redis_info.redis_xid = InvalidTransactionId;
    main_head->redis_info.nwords = 0;
}

static void redo_bucket_free_mapblock(Buffer buffer, const char *data)
{
    uint32 map_id = *(uint32 *)data;
    BktMainHead *main_head = (BktMainHead *)PageGetContents(BufferGetPage(buffer));
    main_head->bkt_map[map_id] = InvalidBlockNumber;
}

static void redo_bucket_add_mapblock(Buffer buffer, const char *data)
{
    uint32 blockid = *(uint32 *)data;
    data += sizeof(uint32);
    BlockNumber mapblock = *(BlockNumber *)data;

    BktMainHead *main_head = (BktMainHead *)PageGetContents(BufferGetPage(buffer));
    main_head->bkt_map[blockid] = mapblock;
}

static void redo_bucket_init_mapblock(Buffer buffer, const char *data)
{
    XLogRecPtr lsn = *(XLogRecPtr *)data;
    bucket_init_map_page(buffer, lsn);
}

static void redo_bucket_add_bkthead(Buffer buffer, const char *data)
{
    int map_entry_id = *(int *)data;
    data += sizeof(int);
    BlockNumber head_block = *(BlockNumber *)data;

    BktHeadMapBlock *map_block = (BktHeadMapBlock *)PageGetContents(BufferGetPage(buffer));
    map_block->head_block[map_entry_id] = head_block;
}

static void redo_space_update_hwm(Buffer buffer, const char *data)
{
    XLogDataUpdateSpaceHWM *xlog_data = (XLogDataUpdateSpaceHWM *)data;
    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetPage(buffer));
    if (map_head->high_water_mark != xlog_data->old_hwm) {
        ereport(PANIC, (errmsg("update space high water mark, old hwm is %u, but should be %u according to xlog",
                               map_head->high_water_mark, xlog_data->old_hwm),
                        errhint("segment-page may have bug")));
    }
    if (map_head->group_count != xlog_data->old_groupcnt) {
        ereport(PANIC, (errmsg("update map group count, old count is %u, but should be %u according to xlog",
                           map_head->group_count, xlog_data->old_groupcnt)));
    }
    map_head->high_water_mark = xlog_data->new_hwm;
    map_head->group_count = xlog_data->new_groupcnt;
}

static void redo_set_inverse_pointer(Buffer buffer, const char *data)
{
    uint32 offset = *(uint32 *)data;
    data += sizeof(uint32);
    ExtentInversePointer iptr = *(ExtentInversePointer *)data;

    ExtentInversePointer *eips = (ExtentInversePointer *)PageGetContents(BufferGetBlock(buffer));
    eips[offset] = iptr;
}

static void redo_shrink_seghead_update(Buffer buffer, const char *data)
{
    XLogMoveExtent *xlog_data = (XLogMoveExtent *)data;
    SegmentHead *seghead = (SegmentHead *)PageGetContents(BufferGetBlock(buffer));
    int extent_id = xlog_data->extent_id;

    if (extent_id < BMT_HEADER_LEVEL0_SLOTS) {
        /* Level0 extent, needs updating segment head */
        SegmentCheck(seghead->level0_slots[extent_id] == xlog_data->old_extent);
        seghead->level0_slots[extent_id] = xlog_data->new_extent;
    }

    SEGMENTTEST(SEGMENT_REDO_UPDATE_SEGHEAD, (errmsg("error happens when replaying segment head update in shrink")));
}

#define REL_NODE_FORMAT(rnode) rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode

/* create hash table using shared memory when first needed */
struct HTAB* redo_create_remain_segs_htbl()
{
    HASHCTL ctl;
    
    errno_t errorno = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(errorno, "", "");
    
    ctl.keysize = sizeof(RemainExtentHashTag);
    ctl.entrysize = sizeof(ExtentTag);
    ctl.hash = tag_hash;
    int flag = HASH_ELEM | HASH_FUNCTION;
    
    return HeapMemInitHash("remain_segs", 1000, DF_MAP_GROUP_EXTENTS, &ctl, flag);
}

static void redo_xlog_log_alloc_seg(Buffer buffer, TransactionId xid)
{
    AutoMutexLock remainSegsLock(&g_instance.xlog_cxt.remain_segs_lock);
    remainSegsLock.lock();

    Assert(TransactionIdIsValid(xid));
    
    if (t_thrd.xlog_cxt.remain_segs == NULL) {
        t_thrd.xlog_cxt.remain_segs = redo_create_remain_segs_htbl();
    }

    BufferDesc *bufDesc = GetBufferDescriptor(buffer - 1);

    RemainExtentHashTag remainExtentHashTag;
    remainExtentHashTag.rnode = bufDesc->tag.rnode;
    remainExtentHashTag.rnode.relNode = bufDesc->tag.blockNum;
    remainExtentHashTag.extentType = bufDesc->tag.rnode.relNode;
    
    bool found = false;
    ExtentTag* extentTag = (ExtentTag *)hash_search(t_thrd.xlog_cxt.remain_segs, (void *)&remainExtentHashTag,
                                                    HASH_ENTER, &found);
    if (found) {
        ereport(WARNING, (errmsg("Segment [%u, %u, %u, %d] already existed in remain segs, Xid %lu,"
                "remainExtentType %u.", REL_NODE_FORMAT(remainExtentHashTag.rnode), extentTag->xid,
                extentTag->remainExtentType)));
    } else {
        extentTag->remainExtentType = ALLOC_SEGMENT;
        extentTag->xid = xid;
        extentTag->forkNum = InvalidForkNumber;
        extentTag->lsn = InvalidXLogRecPtr;
        
        ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("Segment [%u, %u, %u, %d] is alloced, cur xid %lu.",
                REL_NODE_FORMAT(remainExtentHashTag.rnode), xid)));
    }
    remainSegsLock.unLock();
}

static void redo_xlog_forget_alloc_seg(Buffer buffer, const char* data, int data_len)
{
    int first_part_data_len = sizeof(uint16) + sizeof(df_map_page_t);
    if (data_len <= first_part_data_len) {
        Assert(data_len == first_part_data_len);
        return;
    }

    Assert(data_len == (sizeof(uint16) + sizeof(df_map_page_t) + sizeof(BlockNumber)));
    BlockNumber* blk_num = (BlockNumber *)(data + sizeof(uint16) + sizeof(df_map_page_t));
    BufferDesc *bufDesc = GetBufferDescriptor(buffer - 1);

    RemainExtentHashTag remainExtentHashTag;
    remainExtentHashTag.rnode = bufDesc->tag.rnode;
    remainExtentHashTag.rnode.relNode = *blk_num;
    remainExtentHashTag.extentType = bufDesc->tag.rnode.relNode;
    
    AutoMutexLock remain_segs_lock(&g_instance.xlog_cxt.remain_segs_lock);
    remain_segs_lock.lock();
    if (t_thrd.xlog_cxt.remain_segs == NULL) {
        t_thrd.xlog_cxt.remain_segs = redo_create_remain_segs_htbl();
    }

    bool found = false;
    ExtentTag* extentTag = (ExtentTag *)hash_search(t_thrd.xlog_cxt.remain_segs, (void *)&(remainExtentHashTag),
                                                    HASH_REMOVE, &found);
    if (found) {
        ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("Segment [%u, %u, %u, %d] is really freed after"
                "drop trxn committed, xid %lu, remainExtentType %u.", REL_NODE_FORMAT(remainExtentHashTag.rnode),
                extentTag->xid, extentTag->remainExtentType)));
    } else {
        ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("Segment [%u, %u, %u, %d] is not found"
                "in remain segs htbl.", REL_NODE_FORMAT(remainExtentHashTag.rnode))));
    }
    remain_segs_lock.unLock();
}

static void redo_xlog_log_shrink_extent(Buffer buffer, const char* data)
{
    XLogMoveExtent *xlog_data = (XLogMoveExtent *)data;
    SegmentCheck(xlog_data->old_extent != InvalidBlockNumber);

    AutoMutexLock remain_segs_lock(&g_instance.xlog_cxt.remain_segs_lock);
    remain_segs_lock.lock();
    if (t_thrd.xlog_cxt.remain_segs == NULL) {
        t_thrd.xlog_cxt.remain_segs = redo_create_remain_segs_htbl();
    }

    bool found = false;

    RemainExtentHashTag remainExtentHashTag;
    /* spcNode and dbNode are the same as logic rnode */
    remainExtentHashTag.rnode = xlog_data->logic_rnode;
    /* relNode is the old extent that may be leaked */
    remainExtentHashTag.rnode.relNode = xlog_data->old_extent;
    remainExtentHashTag.rnode.bucketNode = SegmentBktId;
    /* extentType is calculated by extent id */
    remainExtentHashTag.extentType = EXTENT_SIZE_TO_TYPE(ExtentSizeByCount(xlog_data->extent_id));
    
    ExtentTag* extentTag = (ExtentTag *)hash_search(t_thrd.xlog_cxt.remain_segs, (void *)&remainExtentHashTag,
                                                    HASH_ENTER, &found);
    if (found) {
        ereport(WARNING, (errmsg("Segment [%u, %u, %u, %d] Extent %u should not be repeatedly founded, xid %lu," 
                "remainExtentType %u", REL_NODE_FORMAT(remainExtentHashTag.rnode), xlog_data->old_extent, extentTag->xid,
                extentTag->remainExtentType)));
    } else {
        extentTag->remainExtentType = SHRINK_EXTENT;
        extentTag->forkNum = xlog_data->forknum;
        
        extentTag->xid = InvalidTransactionId;
        extentTag->lsn = InvalidXLogRecPtr;
        ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("Segment [%u, %u, %u, %d] Extent %u is replaced "
                "during shrinking in shrink extents.", REL_NODE_FORMAT(remainExtentHashTag.rnode), xlog_data->old_extent)));
    }
    remain_segs_lock.unLock();
}

void redo_xlog_deal_alloc_seg(uint8 opCode, Buffer buffer, const char* data, int data_len, TransactionId xid)
{
    if (opCode == SPCXLOG_INIT_SEGHEAD) {
        unsigned char is_seg_head = *(unsigned char *)(data + sizeof(XLogRecPtr));
        if (is_seg_head == 0) {
            return;
        }
        redo_xlog_log_alloc_seg(buffer, xid);
    } else if (opCode == SPCXLOG_INIT_BUCKET_HEAD) {
        redo_xlog_log_alloc_seg(buffer, xid);
    } else if (opCode == SPCXLOG_FREE_BITMAP) {
        redo_xlog_forget_alloc_seg(buffer, data, data_len);
    } else if (opCode == SPCXLOG_SHRINK_SEGHEAD_UPDATE) {
        redo_xlog_log_shrink_extent(buffer, data);
    }
}

void redo_atomic_xlog_dispatch(uint8 opCode, RedoBufferInfo *redo_buf, const char *data)
{
    Buffer buffer = redo_buf->buf;
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("redo_atomic_xlog_dispatch opCode: %u", opCode)));
    if (opCode == SPCXLOG_SET_BITMAP) {
        redo_set_bitmap(redo_buf->blockinfo, buffer, data);
    } else if (opCode == SPCXLOG_FREE_BITMAP) {
        redo_unset_bitmap(buffer, data);
    } else if (opCode == SPCXLOG_MAPHEAD_ALLOCATED_EXTENTS) {
        redo_maphead_allocated_extents(buffer, data);
    } else if (opCode == SPCXLOG_INIT_SEGHEAD) {
        redo_init_seghead(buffer, data);
    } else if (opCode == SPCXLOG_UPDATE_SEGHEAD) {
        redo_update_seghead(buffer, data);
    } else if (opCode == SPCXLOG_NEW_LEVEL0_PAGE) {
        redo_new_level0_page(buffer, data);
    } else if (opCode == SPCXLOG_LEVEL0_PAGE_ADD_EXTENT) {
        redo_level0_page_add_extent(buffer, data);
    } else if (opCode == SPCXLOG_SEGHEAD_ADD_FORK_SEGMENT) {
        redo_seghead_add_fork_segment(buffer, data);
    } else if (opCode == SPCXLOG_UNLINK_SEGHEAD_PTR) {
        redo_unlink_seghead_ptr(buffer, data);
    } else if (opCode == SPCXLOG_INIT_BUCKET_HEAD) {
        redo_init_bucket_main_head(buffer, data);
    } else if (opCode == SPCXLOG_BUCKET_FREE_MAPBLOCK) {
        redo_bucket_free_mapblock(buffer, data);
    } else if (opCode == SPCXLOG_BUCKET_ADD_MAPBLOCK) {
        redo_bucket_add_mapblock(buffer, data);
    } else if (opCode == SPCXLOG_BUCKET_INIT_MAPBLOCK) {
        redo_bucket_init_mapblock(buffer, data);
    } else if (opCode == SPCXLOG_BUCKET_ADD_BKTHEAD) {
        redo_bucket_add_bkthead(buffer, data);
    } else if (opCode == SPCXLOG_SPACE_UPDATE_HWM) {
        redo_space_update_hwm(buffer, data);
    } else if (opCode == SPCXLOG_SET_INVERSE_POINTER) {
        redo_set_inverse_pointer(buffer, data);
    } else if (opCode == SPCXLOG_SEG_MOVE_BUCKETS) {
        redo_move_buckets(buffer, data);
    } else if (opCode == SPCXLOG_BUCKET_ADD_REDISINFO) {
        redo_bucket_add_redisinfo(buffer, data);
    } else {
        SegmentCheck(opCode == SPCXLOG_SHRINK_SEGHEAD_UPDATE);
        redo_shrink_seghead_update(buffer, data);
    }
}

void move_extent_flush_buffer(XLogMoveExtent *xlog_data)
{
    BlockNumber logic_start = ExtentIdToLogicBlockNum(xlog_data->extent_id);
    for (int i=0; i<ExtentSizeByCount(xlog_data->extent_id); i++) {
        BlockNumber blk = logic_start + i;
        if (blk >= xlog_data->nblocks) {
            break;
        }

        Buffer buffer = try_get_moved_pagebuf(&xlog_data->logic_rnode, xlog_data->forknum, blk);
        if (BufferIsValid(buffer)) {
            BlockNumber old_seg_blockno = xlog_data->old_extent + i;
            BlockNumber new_seg_blockno = xlog_data->new_extent + i;

            BufferDesc *buf_desc = BufferGetBufferDescriptor(buffer);
            if (buf_desc->extra->seg_blockno == old_seg_blockno) {
                uint64 buf_state = LockBufHdr(buf_desc);
                if (buf_state & BM_DIRTY) {
                    /* spin-lock should be released before IO */
                    UnlockBufHdr(buf_desc, buf_state);

                    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
                    /* Flush data to the old block */
                    FlushOneBufferIncludeDW(buf_desc);
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                } else {
                    UnlockBufHdr(buf_desc, buf_state);
                }

                /* It's dirty, but we must unpin buffer before InvalidateBuffer */
                UnpinBuffer(buf_desc, true);
                buf_state = LockBufHdr(buf_desc);
                if (RelFileNodeEquals(buf_desc->tag.rnode, xlog_data->logic_rnode) &&
                    buf_desc->tag.forkNum == xlog_data->forknum && buf_desc->tag.blockNum == blk) {
                    InvalidateBuffer(buf_desc);
                } else {
                    UnlockBufHdr(buf_desc, buf_state);
                }
            } else {
                /* Get here only because standby read after we modifiy the segment head */
                SegmentCheck(buf_desc->extra->seg_blockno == new_seg_blockno);
                UnpinBuffer(buf_desc, true);
            }
        }
    }
}

static void redo_atomic_xlog(XLogReaderState *record)
{
    int nbuffers = *(int *)XLogRecGetData(record);
    Buffer buffers[XLR_MAX_BLOCK_ID];
    bool is_need_log_remain_segs = IsNeedLogRemainSegs(record->EndRecPtr);

    XLogMoveExtent move_extent_xlog;
    bool need_flush_buffer_for_shrink = false;

    for (int i = 0; i < nbuffers; i++) {
        RedoBufferInfo redo_buf;

        // If WILL_INT flag is set, force to redo.
        bool will_init = record->blocks[i].flags & BKPBLOCK_WILL_INIT;
        XLogRedoAction redo_action;
        if (will_init) {
            redo_action = SSCheckInitPageXLog(record, i, &redo_buf);
            if (redo_action == BLK_NEEDS_REDO) {
                XLogInitBufferForRedo(record, i, &redo_buf);
            /*
             * If tablespace is dropped, XLogInitBufferForRedo will return an invalid buffer.
             * We do not make a directoy in the place where the tablespace symlink would be like
             * heap-disk storage, otherwise space metadata block (like MapHead block) may be
             * inconsistent.
             */
                redo_action = BufferIsValid(redo_buf.buf) ? BLK_NEEDS_REDO : BLK_NOTFOUND;
            }
        } else {
            redo_action = XLogReadBufferForRedo(record, i, &redo_buf);
        }
        buffers[i] = redo_buf.buf;

        Size len = 0;
        char *data = XLogRecGetBlockData(record, i, &len);
        SegmentCheck(len != 0);
        DecodedXLogBlockOp decoded_op = XLogAtomicDecodeBlockData(data, len);

        if (redo_action == BLK_NEEDS_REDO) {
            for (int j = 0; j < decoded_op.operations; j++) {
                redo_atomic_xlog_dispatch(decoded_op.op[j], &redo_buf, decoded_op.data[j]);
            }

            PageSetLSN(redo_buf.pageinfo.page, redo_buf.lsn);
            SegMarkBufferDirty(redo_buf.buf);
        }
        
        for (int j = 0; j < decoded_op.operations; j++) {
            if (decoded_op.op[j] == SPCXLOG_SHRINK_SEGHEAD_UPDATE) {
                need_flush_buffer_for_shrink = true;
                move_extent_xlog = *(XLogMoveExtent *)(decoded_op.data[j]);
            }

            if (is_need_log_remain_segs) {
                redo_xlog_deal_alloc_seg(decoded_op.op[j], redo_buf.buf,
                                         decoded_op.data[j], decoded_op.data_len[j],
                                         XLogRecGetXid(record));
            }
        }
    }

    SEGMENTTEST(SEGMENT_REPLAY_ATOMIC_OP, (errmsg("error happens during replaying atomic xlog")));

    for (int i = 0; i < nbuffers; i++) {
        if (BufferIsValid(buffers[i])) {
            SegUnlockReleaseBuffer(buffers[i]);
        }
    }

    /* We must handle buffer flush after releasing the segment head buffer to avoid dead lock with standby-read */
    if (need_flush_buffer_for_shrink) {
        SEGMENTTEST(SEGMENT_FLUSH_MOVED_EXTENT_BUFFER, (errmsg("error happens just before flush moved extent buffer")));
        move_extent_flush_buffer(&move_extent_xlog);
    }
}

static void redo_seghead_extend(XLogReaderState *record)
{
    RedoBufferInfo redo_buf;
    XLogRedoAction redo_action = XLogReadBufferForRedo(record, 0, &redo_buf);
    if (redo_action == BLK_NEEDS_REDO) {
        char *data = XLogRecGetBlockData(record, 0, NULL);
        XLogDataSegmentExtend *xlog_data = (XLogDataSegmentExtend *)data;

        SegmentHead *seghead = (SegmentHead *)PageGetContents(redo_buf.pageinfo.page);
        if (seghead->nblocks != xlog_data->old_nblocks) {
            ereport(PANIC, (errmsg("data inconsistent when redo seghead_extend, nblocks is %u on disk, but should be "
                                   "%u according to xlog",
                                   seghead->nblocks, xlog_data->old_nblocks)));
        }
        seghead->nblocks = xlog_data->new_nblocks;

        PageSetLSN(redo_buf.pageinfo.page, redo_buf.lsn);
        SegMarkBufferDirty(redo_buf.buf);
    }

    if (BufferIsValid(redo_buf.buf)) {
        SegUnlockReleaseBuffer(redo_buf.buf);
    }

    if (SSCheckInitPageXLogSimple(record, 1, &redo_buf) == BLK_DONE) {
        return;
    }
    XLogInitBufferForRedo(record, 1, &redo_buf);
    if (BufferIsValid(redo_buf.buf)) {
        memset_s(redo_buf.pageinfo.page, BLCKSZ, 0, BLCKSZ);
        PageSetLSN(redo_buf.pageinfo.page, redo_buf.lsn);
        MarkBufferDirty(redo_buf.buf);
        UnlockReleaseBuffer(redo_buf.buf);
    }
}

/*
 * Ensure this function is executed alone.
 */
static void redo_create_extent_group(XLogReaderState *record)
{
    char *data = XLogRecGetData(record);
    RelFileNode *rnode = (RelFileNode *)data;
    ForkNumber forknum = *(ForkNumber *)(data + sizeof(RelFileNode));

    /* Create tablespace directory on the standby */
    TablespaceCreateDbspace(rnode->spcNode, rnode->dbNode, true);

    /* Create SegSpace object in memory */
    SegSpace *spc = spc_init_space_node(rnode->spcNode, rnode->dbNode);

    eg_init_data_files(&spc->extent_group[EXTENT_TYPE_TO_GROUPID(rnode->relNode)][forknum], true, record->EndRecPtr);
}

static void redo_init_map_page(XLogReaderState *record)
{
    RedoBufferInfo redo_buf;
    if (SSCheckInitPageXLogSimple(record, 0, &redo_buf) == BLK_DONE) {
        return;
    }
    XLogInitBufferForRedo(record, 0, &redo_buf);
    BlockNumber first_page = *(BlockNumber *)XLogRecGetData(record);

    eg_init_bitmap_page_content(redo_buf.pageinfo.page, first_page);
    PageSetLSN(redo_buf.pageinfo.page, redo_buf.lsn);
    SegMarkBufferDirty(redo_buf.buf);
    SegUnlockReleaseBuffer(redo_buf.buf);
}

static void redo_init_inverse_point_page(XLogReaderState *record)
{
    RedoBufferInfo redo_buf;
    if (SSCheckInitPageXLogSimple(record, 0, &redo_buf) == BLK_DONE) {
        return;
    }
    XLogInitBufferForRedo(record, 0, &redo_buf);
    SegPageInit(redo_buf.pageinfo.page, BLCKSZ);
    PageSetLSN(redo_buf.pageinfo.page, redo_buf.lsn);
    SegMarkBufferDirty(redo_buf.buf);
    SegUnlockReleaseBuffer(redo_buf.buf);
}

static void redo_add_new_group(XLogReaderState *record)
{
    RedoBufferInfo redo_buf;
    XLogRedoAction redo_action = XLogReadBufferForRedo(record, 0, &redo_buf);

    if (redo_action == BLK_NEEDS_REDO) {
        xl_new_map_group_info_t *new_map_group = (xl_new_map_group_info_t *)XLogRecGetData(record);

        df_map_head_t *map_head = (df_map_head_t *)PageGetContents(redo_buf.pageinfo.page);
        map_head->group_count = new_map_group->group_count;
        SegmentCheck(map_head->group_count > 0);

        df_map_group_t *map_group = &map_head->groups[map_head->group_count - 1];
        map_group->first_map = new_map_group->first_map_pageno;
        map_group->page_count = new_map_group->group_size;
        map_group->free_page = 0;
        PageSetLSN(redo_buf.pageinfo.page, redo_buf.lsn);
        SegMarkBufferDirty(redo_buf.buf);
    }

    if (BufferIsValid(redo_buf.buf)) {
        SegUnlockReleaseBuffer(redo_buf.buf);
    }
}

static void redo_space_shrink(XLogReaderState *record)
{
    XLogDataSpaceShrink *xlog_data = (XLogDataSpaceShrink *)XLogRecGetData(record);
    SegSpace *spc = spc_open(xlog_data->rnode.spcNode, xlog_data->rnode.dbNode, false);
    if (spc_status(spc) == SpaceDataFileStatus::EMPTY) {
        ereport(LOG, (errmsg("redo space shrink, target space <%u, %u, %u> does not exist", xlog_data->rnode.spcNode,
                             xlog_data->rnode.dbNode, xlog_data->rnode.relNode)));
        return;
    }
    SegExtentGroup *seg = &spc->extent_group[EXTENT_TYPE_TO_GROUPID(xlog_data->rnode.relNode)][xlog_data->forknum];

    char *path = relpathperm(xlog_data->rnode, xlog_data->forknum);
    ereport(LOG, (errmsg("call space shrink files, filename: %s, xlog lsn: %lX", path, record->EndRecPtr)));
    pfree(path);
    spc_shrink_files(seg, xlog_data->target_size, true);

    /* forget metadata buffer that uses physical block number */
    XLogTruncateRelation(seg->rnode, seg->forknum, xlog_data->target_size);
    /* forget data buffer that uses logical block number */
    XLogTruncateSegmentSpace(seg->rnode, seg->forknum, xlog_data->target_size);
}

static void redo_space_drop(XLogReaderState *record)
{
    char *data = (char *)XLogRecGetData(record);
    Oid spcNode = *(Oid *)data;
    Oid dbNode = *(Oid *)(data + sizeof(Oid));
    spc_drop(spcNode, dbNode, true);
    XLogDropSegmentSpace(spcNode, dbNode);
}

void seg_redo_new_page_copy_and_flush(BufferTag *tag, char *data, XLogRecPtr lsn)
{
    char page[BLCKSZ] __attribute__((__aligned__(ALIGNOF_BUFFER))) = {0};

    errno_t er = memcpy_s(page, BLCKSZ, data, BLCKSZ);
    securec_check(er, "\0", "\0");

    PageSetLSN(page, lsn);
    PageSetChecksumInplace(page, tag->blockNum);

    if (FORCE_FINISH_ENABLED) {
        update_max_page_flush_lsn(lsn, t_thrd.proc_cxt.MyProcPid, false);
    }

    if (dw_enabled() && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0) {
        bool flush_old_file = false;
        uint16 pos = seg_dw_single_flush_without_buffer(*tag, (Block)page, &flush_old_file);
        t_thrd.proc->dw_pos = pos;
        t_thrd.proc->flush_new_dw = !flush_old_file;
        SegSpace *spc = spc_open(tag->rnode.spcNode, tag->rnode.dbNode, false);
        SegmentCheck(spc != NULL);
        seg_physical_write(spc, tag->rnode, tag->forkNum, tag->blockNum, page, true);
        if (flush_old_file) {
            g_instance.dw_single_cxt.recovery_buf.single_flush_state[pos] = true;
        } else {
            g_instance.dw_single_cxt.single_flush_state[pos] = true;
        }
        t_thrd.proc->dw_pos = -1;
    } else {
        SegSpace *spc = spc_open(tag->rnode.spcNode, tag->rnode.dbNode, false);
        SegmentCheck(spc != NULL);
        seg_physical_write(spc, tag->rnode, tag->forkNum, tag->blockNum, page, true);
    }
}


/*
 * This xlog only copy data to the new block, without modifying data in buffer. If the logic block being in the
 * buffer pool, its pblk points to the old block. The buffer descriptor can not have the logic blocknumber and the new
 * physical block number because we do not know whether we should use the old or thew new physical block for the same
 * logic block, as later segment head modification can either success or fail.
 */
static void redo_new_page(XLogReaderState *record)
{
    Assert(record != NULL);
    BufferTag *tag = (BufferTag *)XLogRecGetData(record);
    seg_redo_new_page_copy_and_flush(tag, (char *)XLogRecGetData(record) + sizeof(BufferTag), record->EndRecPtr);
}

void segpage_smgr_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_SEG_ATOMIC_OPERATION) {
        redo_atomic_xlog(record);
    } else if (info == XLOG_SEG_SEGMENT_EXTEND) {
        redo_seghead_extend(record);
    } else if (info == XLOG_SEG_CREATE_EXTENT_GROUP) {
        redo_create_extent_group(record);
    } else if (info == XLOG_SEG_INIT_MAPPAGE) {
        redo_init_map_page(record);
    } else if (info == XLOG_SEG_INIT_INVRSPTR_PAGE) {
        redo_init_inverse_point_page(record);
    } else if (info == XLOG_SEG_ADD_NEW_GROUP) {
        redo_add_new_group(record);
    } else if (info == XLOG_SEG_TRUNCATE) {
        redo_truncate(record);
    } else if (info == XLOG_SEG_SPACE_SHRINK) {
        redo_space_shrink(record);
    } else if (info == XLOG_SEG_SPACE_DROP) {
        redo_space_drop(record);
    } else if (info == XLOG_SEG_NEW_PAGE) {
        redo_new_page(record);
    } else {
        ereport(PANIC, (errmsg("smgr_redo: unknown op code %u", info)));
    }
}
