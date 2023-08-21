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
 * extent_group.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segment/extent_group.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xloginsert.h"
#include "access/xlogproc.h"
#include "catalog/storage_xlog.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/smgr/segment.h"

XLogRecPtr log_seg_physical_file_extend(SegLogicFile *segfile);

/* Check MAIN_FORK data files valid or not */
bool eg_df_valid(SegExtentGroup *seg)
{
    AutoMutexLock lock(&seg->lock);
    lock.lock();

    SegLogicFile *sf = seg->segfile;
    SegmentCheck(sf != NULL);

    BlockNumber total_block = seg->segfile->total_blocks;
    if (total_block < DF_FILE_MIN_BLOCKS) {
        /* data file is not large enough */
        ereport(LOG, (errmsg("extent group %s data file is too small. It has %u blocks, but expect at least %u blocks",
                             sf->filename, total_block, (BlockNumber)DF_FILE_MIN_BLOCKS)));
        return false;
    }

    /* Read map head, check 1. block checksum; 2. bit unit */
    char *buffer = NULL;
    char *unaligned_buffer = NULL;
    if (ENABLE_DSS) {
        unaligned_buffer = (char*)palloc(BLCKSZ + ALIGNOF_BUFFER);
        buffer = (char *)BUFFERALIGN(unaligned_buffer);
    } else {
        buffer = (char *)palloc(BLCKSZ);
    }

    df_pread_block(sf, buffer, DF_MAP_HEAD_PAGE);

    if (!PageIsVerified((Page)buffer, DF_MAP_HEAD_PAGE)) {
        ereport(LOG, (errmsg("extent group %s map head block checksum verification failed", sf->filename)));
        if (ENABLE_DSS) {
            pfree(unaligned_buffer);
        } else {
            pfree(buffer);
        }
        return false;
    }

    st_df_map_head *map_head = (st_df_map_head *)PageGetContents((Page)buffer);
    if (map_head->bit_unit != seg->extent_size) {
        ereport(LOG, (errmsg("extent group %s, bit unit in head is %u, but its extent size is %u ", sf->filename,
                             map_head->bit_unit, seg->extent_size)));
        if (ENABLE_DSS) {
            pfree(unaligned_buffer);
        } else {
            pfree(buffer);
        }
        return false;
    }

    if (ENABLE_DSS) {
        pfree(unaligned_buffer);
    } else {
        pfree(buffer);
    }
    
    return true;
}

/*
 * Extent Group Layer
 *
 * Each extent group only allocates extents in a certain size. We use bitmap to manage extents. Each bit is associated
 * with an extent, 0 denotes free and 1 denotes occupied.
 *
 */
void eg_init_df_ctrl(SegExtentGroup *seg)
{
    if (seg->segfile != NULL) {
        return;
    }
    MemoryContext oldcnxt = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    SegLogicFile *sf = (SegLogicFile *)palloc(sizeof(SegLogicFile));
    MemoryContextSwitchTo(oldcnxt);
    df_ctrl_init(sf, seg->rnode, seg->forknum);
    if (!SS_STANDBY_MODE) {
        df_open_files(sf);
    }

    seg->segfile = sf;
}

BlockNumber eg_df_size(SegExtentGroup *seg)
{
    AutoMutexLock lock(&seg->lock);
    lock.lock();

    SegLogicFile *sf = seg->segfile;
    SegmentCheck(sf != NULL);

    return sf->total_blocks;
}

bool eg_df_exists(SegExtentGroup *seg)
{
    AutoMutexLock lock(&seg->lock);
    lock.lock();

    SegLogicFile *sf = seg->segfile;
    SegmentCheck(sf != NULL);

    return (sf->file_num > 0);
}

void eg_create_df(SegExtentGroup *seg)
{
    AutoMutexLock lock(&seg->lock);
    lock.lock();

    SegLogicFile *sf = seg->segfile;
    SegmentCheck(sf != NULL);

    if (sf->file_num == 0) {
        df_create_file(sf, false);
    }
}

void eg_extend_df(SegExtentGroup *seg, BlockNumber target_file_size)
{
    AutoMutexLock lock(&seg->lock);
    lock.lock();

    /* SegLogicFile must be initialized before extending */
    SegLogicFile *sf = seg->segfile;
    SegmentCheck(sf != NULL);

    df_extend(sf, target_file_size);
}

void eg_shrink_df(SegExtentGroup *seg, BlockNumber target)
{
    AutoMutexLock lock(&seg->lock);
    lock.lock();
    
    SegLogicFile *sf = seg->segfile;
    SegmentCheck(sf != NULL);

    df_shrink(sf, target);
}
/*
 * Check current data file status; this function is usually called when the space is first accessed.
 */
SpaceDataFileStatus eg_status(SegExtentGroup *seg)
{
    if (!eg_df_exists(seg)) {
        return SpaceDataFileStatus::EMPTY;
    }
    if (eg_df_valid(seg)) {
        return SpaceDataFileStatus::NORMAL;
    }
    return SpaceDataFileStatus::CRASHED;
}

/* Initialize information in SegExtentGroup structure */
void eg_ctrl_init(SegSpace *spc, SegExtentGroup *seg, int extent_size, ForkNumber forknum)
{
    SegmentCheck(&spc->extent_group[EXTENT_SIZE_TO_GROUPID(extent_size)][forknum] == seg);

    seg->extent_size = extent_size;
    seg->space = spc;
    seg->rnode = {.spcNode = spc->spcNode,
                  .dbNode = spc->dbNode,
                  .relNode = EXTENT_SIZE_TO_TYPE(extent_size),
                  .bucketNode = SegmentBktId,
                  .opt = 0};
    seg->forknum = forknum;
    seg->map_head_entry = DF_MAP_HEAD_PAGE;

    pthread_mutex_init(&seg->lock, NULL);
    eg_init_df_ctrl(seg);
}

void eg_init_datafile_head(SegExtentGroup *seg)
{
    /*
     * The first block in the data file is reserved, so that we can use it to store metadata of the data file
     * if necessary in the future.
     */
}

void eg_init_map_head_page_content(Page map_head_page, int extent_size)
{
    SegPageInit(map_head_page, BLCKSZ);
    char *content = PageGetContents(map_head_page);
    df_map_head_t *map_head = (df_map_head_t *)content;
    map_head->bit_unit = extent_size;
    map_head->group_count = 0;
    map_head->free_group = 0;
    map_head->allocated_extents = 0;

    PageHeader page_header = (PageHeader)map_head_page;
    page_header->pd_lower += sizeof(df_map_head_t);
}

static void eg_init_map_head(SegExtentGroup *seg, XLogRecPtr rec_ptr)
{
    BlockNumber pageno = DF_MAP_HEAD_PAGE;
    Page page = NULL;
    char* unaligned_page = NULL;

    if (ENABLE_DSS) {
        unaligned_page = (char*)palloc(BLCKSZ + ALIGNOF_BUFFER);
        page = (Page)BUFFERALIGN(unaligned_page);
    } else {
        page = (Page)palloc(BLCKSZ);
    }

    errno_t er = memset_s((void *)page, BLCKSZ, 0, BLCKSZ);
    securec_check(er, "", "");

    eg_init_map_head_page_content(page, seg->extent_size);

    PageSetLSN(page, rec_ptr);
    PageSetChecksumInplace(page, pageno);
    df_pwrite_block(seg->segfile, (char *)page, pageno);

    if (ENABLE_DSS) {
        pfree(unaligned_page);
    } else {
        pfree(page);
    }

    seg->map_head_entry = pageno;
    seg->map_head = NULL;
}

void eg_clean_data_files(SegExtentGroup *seg)
{
    df_unlink(seg->segfile);
}

void eg_init_data_files(SegExtentGroup *eg, bool redo, XLogRecPtr rec_ptr)
{
    SEGMENTTEST(EXTENT_GROUP_INIT_DATA, (errmsg("EXTENT_GROUP_INIT_DATA %s: the first time for create segment tb!\n",
        g_instance.attr.attr_common.PGXCNodeName)));

    /* create file will ensure the file has enough space */
    df_create_file(eg->segfile, redo);
    eg_init_datafile_head(eg);     // initialize data file
    eg_init_map_head(eg, rec_ptr); // initialize map head
    df_fsync(eg->segfile);
}

static inline Buffer eg_read_maphead_buffer(SegExtentGroup *seg)
{
    return ReadBufferFast(seg->space, seg->rnode, seg->forknum, seg->map_head_entry, RBM_NORMAL);
}

/*
 * Find the map page which manages the given page.
 *
 * input: seg, page_id, map_head
 * return: false if the given page id exceeds the current map management.
 */
SpaceMapLocation eg_locate_map_by_pageid(SegExtentGroup *seg, BlockNumber page_id, df_map_head_t *map_head)
{
    SpaceMapLocation result;
    errno_t er = memset_s(&result, sizeof(SpaceMapLocation), 0, sizeof(SpaceMapLocation));
    securec_check(er, "", "");

    for (int i = 0; i < map_head->group_count; i++) {
        result.group = map_head->groups[i];
        result.map_group = i;
        BlockNumber page_start = map_head->groups[i].first_map + DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE;
        BlockNumber page_end = page_start + DF_MAP_GROUP_EXTENTS * seg->extent_size;

        if (page_start <= page_id && page_id < page_end) {
            result.map_id = (page_id - page_start) / map_head->bit_unit / DF_MAP_BIT_CNT;
            result.bit_id = ((page_id - page_start) / map_head->bit_unit) % DF_MAP_BIT_CNT;
            return result;
        }
    }

    return result;
}

void eg_set_bitmap_page(df_map_page_t *map_page, uint16 bit_id)
{
    SegmentCheck(DF_MAP_FREE(map_page->bitmap, bit_id));

    DF_MAP_SET(map_page->bitmap, bit_id);
    map_page->free_bits--;
    if (map_page->free_begin == bit_id) {
        map_page->free_begin++;
    }
    if (map_page->dirty_last < bit_id) {
        map_page->dirty_last = bit_id;
    }
}

void eg_init_bitmap_page_content(Page bitmap_page, BlockNumber first_page)
{
    SegPageInit(bitmap_page, BLCKSZ);

    df_map_page_t *df_map_page = (df_map_page_t *)PageGetContents(bitmap_page);
    df_map_page->free_begin = 0;
    df_map_page->dirty_last = 0;
    df_map_page->free_bits = DF_MAP_BIT_CNT;
    df_map_page->first_page = first_page;

    PageHeader bitmap_page_header = (PageHeader)bitmap_page;
    SegmentCheck(bitmap_page_header->pd_upper == BLCKSZ);
    bitmap_page_header->pd_lower = bitmap_page_header->pd_upper;
}

/*
 * Try update high water mark. It does not need WAL; Redo procedure will update hwm when updating 'allocated_extents'
 */
void try_update_hwm(df_map_head_t *map_head, uint32 hwm)
{
    if (map_head->high_water_mark < hwm) {
        map_head->high_water_mark = hwm;
    }
}

/*
 * MapPage and MapHeadPage should be locked.
 */
BlockNumber eg_alloc_extent_from_map_internal(SegExtentGroup *seg, Buffer map_buffer, uint16 bitid,
                                              BlockNumber *target_file_size, uint32 map_group, uint32 free_page)
{
    df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetPage(map_buffer));
    BlockNumber pagenum = map_page->first_page + ((BlockNumber)(bitid)) * seg->map_head->bit_unit;

    /* check whether the allocated block exceeds the file size */
    if ((pagenum + seg->extent_size) > seg->segfile->total_blocks) {
        *target_file_size = (pagenum + seg->extent_size);
        SegUnlockReleaseBuffer(map_buffer);
        return InvalidBlockNumber;
    }

    /* set the bitmap and other metadata. */
    eg_set_bitmap_page(map_page, bitid);

    /* update map head metadata */
    seg->map_head->allocated_extents++;
    seg->map_head->free_group = map_group;
    seg->map_head->groups[map_group].free_page = free_page;
    try_update_hwm(seg->map_head, pagenum + seg->extent_size);

    /* XLOG issue */
    {
        XLogAtomicOpRegisterBuffer(map_buffer, REGBUF_KEEP_DATA, SPCXLOG_SET_BITMAP, XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
        XLogAtomicOpRegisterBufData((char *)&bitid, sizeof(uint16));
        XLogAtomicOpRegisterBufData((char *)map_page, sizeof(df_map_page_t));

        XLogDataSpaceAllocateExtent xlog_data;
        xlog_data.hwm = seg->map_head->high_water_mark;
        xlog_data.allocated_extents = seg->map_head->allocated_extents;
        xlog_data.free_group = map_group;
        xlog_data.free_page = free_page;
        XLogAtomicOpRegisterBuffer(seg->map_head_buffer, REGBUF_KEEP_DATA, SPCXLOG_MAPHEAD_ALLOCATED_EXTENTS,
                                   XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
        XLogAtomicOpRegisterBufData((char *)&xlog_data, sizeof(xlog_data));
    }

    return pagenum;
}

BlockNumber eg_alloc_extent_from_map(SegExtentGroup *seg, BlockNumber map_page_id, BlockNumber *target_file_size,
    uint32 map_group, uint32 free_page)
{
    Buffer map_buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, map_page_id, RBM_NORMAL);
    LockBuffer(map_buffer, BUFFER_LOCK_EXCLUSIVE);

    df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetPage(map_buffer));

    if (map_page->free_bits == 0) {
        SegUnlockReleaseBuffer(map_buffer);
        return InvalidBlockNumber;
    }

    /* Find a free bit */
    uint16 curr = map_page->free_begin;
    while (curr < DF_MAP_BIT_CNT) {
        if (DF_MAP_FREE(map_page->bitmap, curr)) {
            break;
        }
        curr++;
    }

    if (curr >= DF_MAP_BIT_CNT) {
        /* current map does not contain a free bit */
        SegUnlockReleaseBuffer(map_buffer);
        return InvalidBlockNumber;
    }

    /* internal function will release and unlock the MapPage buffer */
    return eg_alloc_extent_from_map_internal(seg, map_buffer, curr, target_file_size, map_group, free_page);
}

bool eg_alloc_preassigned_block(SegExtentGroup *seg, BlockNumber preassigned_block, BlockNumber *target_file_size)
{
    SpaceMapLocation location = eg_locate_map_by_pageid(seg, preassigned_block, seg->map_head);

    if (MapLocationIsInvalid(location)) {
        return false;
    }
    uint16 bit_id = location.bit_id;

    BlockNumber map_blocknum = location.map_id + location.group.first_map;

    Buffer map_buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, map_blocknum, RBM_NORMAL);
    LockBuffer(map_buffer, BUFFER_LOCK_EXCLUSIVE);
    df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetPage(map_buffer));

    if (DF_MAP_FREE(map_page->bitmap, bit_id)) {
        BlockNumber pagenum = map_page->first_page + bit_id * seg->map_head->bit_unit;
        // check whether the allocated block exceeds the file size
        if ((pagenum + seg->extent_size) > seg->segfile->total_blocks) {
            *target_file_size = pagenum + seg->extent_size;
            SegUnlockReleaseBuffer(map_buffer);
            return false;
        }
    } else {
        ereport(PANIC, (errmsg("The preassigned segment block %u is already in use", preassigned_block)));
    }
    uint32 free_group = seg->map_head->free_group;
    uint32 free_page = seg->map_head->groups[free_group].free_page;

    SegmentCheck(free_group < seg->map_head->group_count && free_page < seg->map_head->groups[free_group].page_count);
    (void)eg_alloc_extent_from_map_internal(seg, map_buffer, bit_id, target_file_size, free_group, free_page);
    return true;
}

void eg_init_bitmap_page(SegExtentGroup *seg, BlockNumber pageno, BlockNumber first_page)
{
    Buffer buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, pageno, RBM_ZERO);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    START_CRIT_SECTION();

    SegMarkBufferDirty(buffer);
    eg_init_bitmap_page_content(BufferGetPage(buffer), first_page);

    /* XLog Issue */
    XLogBeginInsert();
    XLogRegisterData((char *)&first_page, sizeof(BlockNumber));
    XLogRegisterBuffer(0, buffer, REGBUF_WILL_INIT);
    XLogRecPtr rec_ptr = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_INIT_MAPPAGE, SegmentBktId);
    PageSetLSN(BufferGetPage(buffer), rec_ptr);

    END_CRIT_SECTION();

    SegUnlockReleaseBuffer(buffer);
}

void eg_init_invrsptr_page(SegExtentGroup *seg, BlockNumber pageno)
{
    Buffer buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, pageno, RBM_ZERO_AND_LOCK);
    Page page = BufferGetPage(buffer);

    START_CRIT_SECTION();
    SegMarkBufferDirty(buffer);

    SegPageInit(page, BLCKSZ);

    XLogBeginInsert();
    XLogRegisterBuffer(0, buffer, REGBUF_WILL_INIT);
    XLogRecPtr rec_ptr = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_INIT_INVRSPTR_PAGE, SegmentBktId);
    PageSetLSN(page, rec_ptr);

    END_CRIT_SECTION();
    SegUnlockReleaseBuffer(buffer);
}

/* map_block should be locked */
BlockNumber eg_next_group_start(df_map_head_t *map_head)
{
    SegmentCheck(map_head->group_count > 0);

    df_map_group_t last_map_group = map_head->groups[map_head->group_count - 1];
    return last_map_group.first_map + last_map_group.page_count + IPBLOCK_GROUP_SIZE +
           DF_MAP_GROUP_EXTENTS * (uint32)map_head->bit_unit;
}

void eg_add_map_group(SegExtentGroup *seg, BlockNumber pageno, uint8 group_size, int old_group_count)
{
    ereport(LOG, (errmsg("Extent group (%u, %u, %u) add new group #%d, start pageno: %u ", seg->rnode.spcNode,
                         seg->rnode.dbNode, seg->rnode.relNode, old_group_count, pageno)));
    Buffer map_head_buffer = seg->map_head_buffer;
    Page page = BufferGetPage(map_head_buffer);
    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(page);

    if (old_group_count < map_head->group_count) {
        /* Some one else extend the extent group for us */
        return;
    }
    SegmentCheck(old_group_count == map_head->group_count);
    SegmentCheck(group_size == DF_MAP_GROUP_SIZE);

    BlockNumber start_map_no = pageno;
    BlockNumber data_start = pageno + group_size + IPBLOCK_GROUP_SIZE;
    for (uint32 i = 0; i < group_size; i++) {
        eg_init_bitmap_page(seg, pageno, data_start);
        data_start += DF_MAP_BIT_CNT * seg->extent_size;
        pageno++;
    }

    for (uint32 i = 0; i < IPBLOCK_GROUP_SIZE; i++) {
        eg_init_invrsptr_page(seg, pageno);
        pageno++;
    }
    SEGMENTTEST(EXTENT_GROUP_ADD_NEW_GROUP, (errmsg("EXTENT_GROUP_ADD_NEW_GROUP %s: add new group success!\n",
        g_instance.attr.attr_common.PGXCNodeName)));
    /* XLog issue */
    {
        START_CRIT_SECTION();
        /* Update map head */
        df_map_group_t *bitmap_group = &map_head->groups[map_head->group_count];
        map_head->group_count++;
        bitmap_group->first_map = start_map_no;
        bitmap_group->page_count = group_size;
        bitmap_group->free_page = 0;

        SegMarkBufferDirty(map_head_buffer);

        xl_new_map_group_info_t new_map_group_info = {.first_map_pageno = start_map_no,
                                                      .extent_size = seg->extent_size,
                                                      .group_count = map_head->group_count,
                                                      .group_size = group_size};
        XLogBeginInsert();
        XLogRegisterData((char *)&new_map_group_info, sizeof(xl_new_map_group_info_t));

        XLogRegisterBuffer(0, map_head_buffer, REGBUF_STANDARD);
        XLogRecPtr recptr = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_ADD_NEW_GROUP, SegmentBktId);

        PageSetLSN(page, recptr);
        END_CRIT_SECTION();
    }
    SEGMENTTEST(EXTENT_GROUP_ADD_NEW_GROUP_XLOG, (errmsg("EXTENT_GROUP_ADD_NEW_GROUP_XLOG %s: add new group xlog success!\n",
        g_instance.attr.attr_common.PGXCNodeName)));
}

/*
 * Find a free extent in the current extent group. If all space is used up, return InvalidBlockNumber.
 *
 * Before calling this function, seg->map_head_buffer must be set and locked exclusively.
 * On return, if result is InvalidBlockNumber, there are two reasons:
 *  1. *target_file_size > 0, we need extend physical data file size
 *  2. *target_file_size = 0, we need add a new map group.
 */
BlockNumber eg_try_alloc_extent(SegExtentGroup *seg, BlockNumber preassigned_block, BlockNumber *target_file_size)
{
    *target_file_size = 0;
    df_map_group_t *map_group = NULL;

    BlockNumber curr_map_bno = 0;
    BlockNumber result = InvalidBlockNumber;

    df_map_head_t *map_head = seg->map_head;
    seg->map_head = map_head;

    SegmentCheck(map_head->group_count > 0 &&
                 map_head->free_group < map_head->group_count);

    /* preassigned block */
    if (BlockNumberIsValid(preassigned_block)) {
        if (eg_alloc_preassigned_block(seg, preassigned_block, target_file_size)) {
            return preassigned_block;
        } else {
            return InvalidBlockNumber;
        }
    }

    for (int i = map_head->free_group; i < map_head->group_count; i++) {
        map_group = &map_head->groups[i];
        curr_map_bno = map_group->first_map + map_group->free_page;

        SegmentCheck(map_group->free_page < map_group->page_count);
        for (int j = map_group->free_page; j < map_group->page_count; j++) {
            result = eg_alloc_extent_from_map(seg, curr_map_bno, target_file_size, i, j);
            if (result != InvalidBlockNumber) {
                return result;
            }

            if (*target_file_size > 0) {
                return InvalidBlockNumber;
            }
            curr_map_bno++;
        }
    }

    /*
     * We can not allocate extent, and do not need extend file, it means that there are
     * some blocks at the end of the data file is not managed by MapPage. Thus we need
     * to add a new map group.
     */
    BlockNumber next_group_data_start = eg_next_group_start(map_head) + DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE;
    if (next_group_data_start > seg->segfile->total_blocks) {
        *target_file_size = next_group_data_start;
    }
    return InvalidBlockNumber;
}

bool eg_empty(SegExtentGroup *seg)
{
    if (!eg_df_exists(seg)) {
        return true;
    }
    int result;
    Buffer buffer = eg_read_maphead_buffer(seg);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    seg->map_head = (df_map_head_t *)PageGetContents(BufferGetPage(buffer));
    result = seg->map_head->allocated_extents;

    SegUnlockReleaseBuffer(buffer);

#ifdef USE_ASSERT_CHECKING
    if (result != 0) {
        ereport(LOG, (errmsg("Extent group %d is not empty, there are %d extents used", seg->extent_size, result)));
    }
#endif
    return result == 0;
}

SegmentSpaceStat eg_storage_stat(SegExtentGroup *seg)
{
    if (!eg_df_exists(seg)) {
        return {.extent_size = (uint32)seg->extent_size,
                .forknum = seg->forknum,
                .total_blocks = 0,
                .meta_data_blocks = 0,
                .used_data_blocks = 0,
                .utilization = 0,
                .high_water_mark = 0};
    }
    Buffer buffer = eg_read_maphead_buffer(seg);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetPage(buffer));

    BlockNumber total_blocks = seg->segfile->total_blocks;

    // map group start from DF_MAP_HEAD_PAGE + 1
    BlockNumber meta_data_blocks = DF_MAP_HEAD_PAGE + 1;
    for (int i = 0; i < map_head->group_count; i++) {
        meta_data_blocks += DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE;
    }
    BlockNumber used_data_blocks = map_head->allocated_extents * seg->extent_size;

    SegmentSpaceStat result = {.extent_size = (uint32)seg->extent_size,
                               .forknum = seg->forknum,
                               .total_blocks = total_blocks,
                               .meta_data_blocks = meta_data_blocks,
                               .used_data_blocks = used_data_blocks,
                               .utilization = float(meta_data_blocks + used_data_blocks) / total_blocks,
                               .high_water_mark = map_head->high_water_mark};

    SegUnlockReleaseBuffer(buffer);
    return result;
}

void GetAllInversePointer(SegExtentGroup *seg, uint32 *cnt, ExtentInversePointer **iptrs, BlockNumber **extents)
{
    if (!eg_df_exists(seg)) {
        *cnt = 0;
        *iptrs = NULL;
        *extents = NULL;
        return;
    }
    uint32 len = 1024;
    *iptrs = (ExtentInversePointer *)palloc(sizeof(ExtentInversePointer) * len);
    *extents = (BlockNumber *)palloc(sizeof(BlockNumber) * len);
    *cnt = 0;

    Buffer buffer = eg_read_maphead_buffer(seg);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetPage(buffer));

    /* copy meta-data from map head, to avoid locking it for long time. */
    int group_count = map_head->group_count;
    df_map_group_t groups[DF_MAX_MAP_GROUP_CNT];
    for (int i = 0; i < group_count; i++) {
        groups[i] = map_head->groups[i];
    }
    SegUnlockReleaseBuffer(buffer);

    Buffer ipbuf = InvalidBuffer;

    for (int i = 0; i < group_count; i++) {
        BlockNumber first_map = groups[i].first_map;
        uint8 page_count = groups[i].page_count;
        SegmentCheck(page_count == DF_MAP_GROUP_SIZE);

        for (uint8 j = 0; j < page_count; j++) {
            BlockNumber map_block = first_map + j;
            Buffer map_buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, map_block, RBM_NORMAL);
            LockBuffer(map_buffer, BUFFER_LOCK_SHARE);

            df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetPage(map_buffer));

            for (uint16 k = 0; k <= map_page->dirty_last; k++) {
                if (DF_MAP_NOT_FREE(map_page->bitmap, k)) {
                    BlockNumber extent = map_page->first_page + k * seg->extent_size;
                    ExtentInversePointer iptr = GetInversePointer(seg, extent, &ipbuf);

                    (*cnt)++;
                    if (*cnt > len) {
                        if (len < 16384) {
                            len *= 2;
                        } else {
                            len += 16384;
                        }
                        *iptrs = (ExtentInversePointer *)repalloc(*iptrs, sizeof(ExtentInversePointer) * len);
                        *extents = (BlockNumber *)repalloc(*extents, sizeof(BlockNumber) * len);
                    }
                    (*iptrs)[(*cnt) - 1] = iptr;
                    (*extents)[(*cnt) - 1] = extent;
                }
            }

            SegUnlockReleaseBuffer(map_buffer);
        }
    }
    if (BufferIsValid(ipbuf)) {
        SegReleaseBuffer(ipbuf);
    }
}

void eg_create_if_necessary(SegExtentGroup *seg)
{
    if (!eg_df_exists(seg)) {
        AutoMutexLock lock(&seg->lock);
        lock.lock();

        if (seg->segfile->file_num > 0) {
            /* Someone else has created it */
            return;
        }

        TablespaceCreateDbspace(seg->rnode.spcNode, seg->rnode.dbNode, false);
        /* Ensure tablespace limits at first. */
        uint64 requestSize = DF_FILE_EXTEND_STEP_SIZE;
        TableSpaceUsageManager::IsExceedMaxsize(seg->rnode.spcNode, requestSize, true);

        START_CRIT_SECTION();
        t_thrd.pgxact->delayChkpt = true;

        XLogBeginInsert();
        XLogRegisterData((char *)&seg->rnode, sizeof(RelFileNode));
        XLogRegisterData((char *)&seg->forknum, sizeof(ForkNumber));
        XLogRecPtr xlog = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_CREATE_EXTENT_GROUP, SegmentBktId);
        XLogWaitFlush(xlog);

        eg_init_data_files(seg, false, xlog);
        SEGMENTTEST(EXTENT_GROUP_CREATE_EXTENT, (errmsg("EXTENT_GROUP_CREATE_EXTENT %s: create segment file success!\n",
            g_instance.attr.attr_common.PGXCNodeName)));
        t_thrd.pgxact->delayChkpt = false;
        END_CRIT_SECTION();
    }
}

/*
 * Extent Group Layer APIS: allocate/free extents; auto extend if the space is used up.
 */
BlockNumber eg_alloc_extent(SegExtentGroup *seg, BlockNumber preassigned_block, ExtentInversePointer iptr)
{
    eg_create_if_necessary(seg);

    BlockNumber blocknum = InvalidBlockNumber;
    BlockNumber file_target_size = InvalidBlockNumber;
    for (;;) {
        seg->map_head_buffer = eg_read_maphead_buffer(seg);
        LockBuffer(seg->map_head_buffer, BUFFER_LOCK_EXCLUSIVE);
        seg->map_head = (df_map_head_t *)PageGetContents(BufferGetBlock(seg->map_head_buffer));
        if (seg->map_head->group_count == 0) {
            // create the first map group
            eg_add_map_group(seg, DF_MAP_HEAD_PAGE + 1, DF_MAP_GROUP_SIZE, 0);
        }

        blocknum = eg_try_alloc_extent(seg, preassigned_block, &file_target_size);
        if (blocknum != InvalidBlockNumber) {
            // Get an extent, we do not have to release the map head buffer. XLogAtomicOpCommit will do it for us.
            // set inverse point and return
            SetInversePointer(seg, blocknum, iptr);
            SEGMENTTEST(EXTENT_GROUP_CRITICAL_SECTION, (errmsg("EXTENT_GROUP_CRITICAL_SECTION %s: alloc extent end, begin critical section!\n",
                g_instance.attr.attr_common.PGXCNodeName)));
            return blocknum;
        }

        if (file_target_size > 0) {
            // we need extend data file
            SegUnlockReleaseBuffer(seg->map_head_buffer);
            eg_extend_df(seg, file_target_size);
        } else {
            // we need an extra extent group
            BlockNumber group_start = eg_next_group_start(seg->map_head);
            eg_add_map_group(seg, group_start, DF_MAP_GROUP_SIZE, seg->map_head->group_count);
            SegUnlockReleaseBuffer(seg->map_head_buffer);
        }
    }
}

/*
 * Free extent should be reentrant
 */
void eg_unset_bitmap_page(df_map_page_t *map_page, uint16 bit_id)
{
    SegmentCheck(DF_MAP_NOT_FREE(map_page->bitmap, bit_id));

    DF_MAP_UNSET(map_page->bitmap, bit_id);
    map_page->free_bits++;
    if (map_page->free_begin > bit_id) {
        map_page->free_begin = bit_id;
    }
}

/*
 * Free an extent in the extent group. 'allocated_extents' will be updated.
 */
void eg_free_extent(SegExtentGroup *seg, BlockNumber blocknum)
{
    seg->map_head_buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, seg->map_head_entry, RBM_NORMAL);
    LockBuffer(seg->map_head_buffer, BUFFER_LOCK_EXCLUSIVE);
    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetPage(seg->map_head_buffer));

    SpaceMapLocation location = eg_locate_map_by_pageid(seg, blocknum, map_head);
    if (MapLocationIsInvalid(location)) {
        ereport(PANIC, (errmsg("Free an already freed extent")));
    }
    uint16 bit_id = location.bit_id;

    BlockNumber map_blocknum = location.map_id + location.group.first_map;
    Buffer map_buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, map_blocknum, RBM_NORMAL);
    LockBuffer(map_buffer, BUFFER_LOCK_EXCLUSIVE);

    df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetPage(map_buffer));
    eg_unset_bitmap_page(map_page, bit_id);
    map_head->allocated_extents--;

    /* Update free group and free page if needed */
    if (map_head->free_group > location.map_group) {
        map_head->free_group = location.map_group;
    }
    if (map_head->groups[location.map_group].free_page > location.map_id) {
        map_head->groups[location.map_group].free_page = location.map_id;
    }

    // XLog issue
    {
        XLogAtomicOpRegisterBuffer(map_buffer, REGBUF_KEEP_DATA, SPCXLOG_FREE_BITMAP,
                                   XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
        XLogAtomicOpRegisterBufData((char *)&bit_id, sizeof(bit_id));
        XLogAtomicOpRegisterBufData((char *)map_page, sizeof(df_map_page_t));
        XLogAtomicOpRegisterBufData((char *)&blocknum, sizeof(BlockNumber));

        XLogAtomicOpRegisterBuffer(seg->map_head_buffer, REGBUF_KEEP_DATA, SPCXLOG_MAPHEAD_ALLOCATED_EXTENTS,
                                   XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
        XLogDataSpaceAllocateExtent xlog_data;
        xlog_data.hwm = map_head->high_water_mark;
        xlog_data.allocated_extents = map_head->allocated_extents;
        xlog_data.free_group = map_head->free_group;
        xlog_data.free_page = map_head->groups[map_head->free_group].free_page;
        XLogAtomicOpRegisterBufData((char *)&xlog_data, sizeof(xlog_data));
    }
}
