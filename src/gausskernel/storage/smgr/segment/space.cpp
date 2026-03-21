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
 * space.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segment/space.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xloginsert.h"
#include "access/double_write.h"
#include "catalog/indexing.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage_xlog.h"
#include "catalog/pg_partition_fn.h"
#include "commands/tablespace.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/buf/buf_internals.h"
#include "storage/lmgr.h"
#include "storage/smgr/segment.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/relfilenodemap.h"
#include "pgxc/execRemote.h"
#include "ddes/dms/ss_transaction.h"
#include "ddes/dms/ss_aio.h"
#include "storage/file/fio_device.h"
#include "storage/cfs/cfs_md.h"

static void SSInitSegLogicFile(SegSpace *spc);

void spc_lock(SegSpace *spc)
{
    PthreadMutexLock(t_thrd.utils_cxt.CurrentResourceOwner, &spc->lock, true);
}

void spc_unlock(SegSpace *spc)
{
    PthreadMutexUnlock(t_thrd.utils_cxt.CurrentResourceOwner, &spc->lock, true);
}

/*
 * Segment Space Layer
 *
 * Find the extent group according to the given extent size, and call corresponding functions of
 * the extent group.
 */
BlockNumber spc_alloc_extent(SegSpace *spc, int extent_size, ForkNumber forknum, BlockNumber preassigned_block,
                             ExtentInversePointer iptr)
{
    spc_lock(spc);
    int egid = EXTENT_SIZE_TO_GROUPID(extent_size);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];

    BlockNumber extent = eg_alloc_extent(seg, preassigned_block, iptr);

    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("[Segment Page] allocate extent [%u, %u %u] %u", extent_size,
                                                         spc->spcNode, spc->dbNode, extent)));
    spc_unlock(spc);
    return extent;
}

/* Return map buffer, callers must release and unlock the buffer, and set correct LSN */
void spc_free_extent(SegSpace *spc, int extent_size, ForkNumber forknum, BlockNumber blocknum)
{
    spc_lock(spc);
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("[Segment Page] free extent [%u, %u %u] %u", extent_size,
                                                         spc->spcNode, spc->dbNode, blocknum)));

    int egid = EXTENT_SIZE_TO_GROUPID(extent_size);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];
    SegmentCheck(eg_df_exists(seg) == true);

    eg_free_extent(seg, blocknum);
    spc_unlock(spc);
}

void spc_read_block(SegSpace *spc, RelFileNode relNode, ForkNumber forknum, char *buffer, BlockNumber blocknum)
{
    SMgrRelation rel = smgropen(relNode, InvalidBackendId, GetColumnNum(forknum));
    int egid = EXTENT_TYPE_TO_GROUPID(relNode.relNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];

    if (IS_SEG_COMPRESSED_RNODE(relNode, forknum) && SegIsDataBlock(blocknum, seg->extent_size)) {
        int fd = df_get_fd(seg->segfile, blocknum);
        CfsReadPage(rel, relNode, fd, seg->extent_size, forknum, blocknum, buffer, SEG_STORAGE);
    } else {
        df_pread_block(seg->segfile, buffer, blocknum);
    }
    return;
}

void spc_write_block(SegSpace *spc, RelFileNode relNode, ForkNumber forknum, const char *buffer,
                     BlockNumber blocknum)
{
    SMgrRelation rel = smgropen(relNode, InvalidBackendId, GetColumnNum(forknum));
    int egid = EXTENT_TYPE_TO_GROUPID(relNode.relNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];

    if (IS_SEG_COMPRESSED_RNODE(relNode, forknum) && SegIsDataBlock(blocknum, seg->extent_size)) {
        int fd = df_get_fd(seg->segfile, blocknum);
        CfsWritePage(rel, relNode, fd, seg->extent_size, forknum, blocknum, buffer, false, SEG_STORAGE);
        int sliceno = DF_OFFSET_TO_SLICENO(((off_t)blocknum) * BLCKSZ);
        seg_register_dirty_file(seg->segfile, sliceno);
    } else {
        df_pwrite_block(seg->segfile, buffer, blocknum);
    }
}

int32 spc_aio_prep_pwrite(SegSpace *spc, RelFileNode relNode, ForkNumber forknum, BlockNumber blocknum,
    const char *buffer, void *iocb_ptr, void *tempAioExtra)
{
    int egid = EXTENT_TYPE_TO_GROUPID(relNode.relNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];

    off_t offset = ((off_t)blocknum) * BLCKSZ;
    int sliceno = DF_OFFSET_TO_SLICENO(offset);
    off_t roffset = DF_OFFSET_TO_SLICE_OFFSET(offset);

    SegPhysicalFile spf = df_get_physical_file(seg->segfile, sliceno, blocknum);
    int32 ret;
    if (is_dss_fd(spf.fd)) {
        ((PgwrAioExtraData *)tempAioExtra)->aio_fd = spf.fd;
        ret = dss_aio_prep_pwrite(iocb_ptr, spf.fd, (void *)buffer, BLCKSZ, roffset);
    } else {
        io_prep_pwrite((struct iocb *)iocb_ptr, spf.fd, (void *)buffer, BLCKSZ, roffset);
        ret = DSS_SUCCESS;
    }

    return ret;
}

void spc_writeback(SegSpace *spc, RelFileNode relNode, ForkNumber forknum, BlockNumber blocknum,
                   BlockNumber nblocks)
{
    SMgrRelation rel = smgropen(relNode, InvalidBackendId, GetColumnNum(forknum));
    int egid = EXTENT_TYPE_TO_GROUPID(relNode.relNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];

    SegLogicFile *sf = spc->extent_group[egid][forknum].segfile;

    if (IS_SEG_COMPRESSED_RNODE(relNode, forknum) && SegIsDataBlock(blocknum, seg->extent_size)) {
        while (nblocks > 0) {
            int fd = df_get_fd(sf, blocknum);
            auto nflushed = CfsWriteBack(rel, relNode, fd, seg->extent_size, forknum, blocknum, nblocks, SEG_STORAGE);
            if (nflushed == InvalidBlockNumber) {
                return;
            }
            nblocks -= nflushed;
            blocknum += nflushed;
        }
    } else {
        df_flush_data(sf, blocknum, nblocks);
    }
}

BlockNumber spc_size(SegSpace *spc, BlockNumber egRelNode, ForkNumber forknum)
{
    int egid = EXTENT_TYPE_TO_GROUPID(egRelNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];
    return eg_df_size(seg);
}

void spc_datafile_create(SegSpace *spc, BlockNumber egRelNode, ForkNumber forknum)
{
    int egid = EXTENT_TYPE_TO_GROUPID(egRelNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];
    eg_create_df(seg);
}

void spc_extend_file(SegSpace *spc, BlockNumber egRelNode, ForkNumber forknum, BlockNumber blkno)
{
    int egid = EXTENT_TYPE_TO_GROUPID(egRelNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];
    eg_extend_df(seg, blkno);
}

bool spc_datafile_exist(SegSpace *spc, BlockNumber egRelNode, ForkNumber forknum)
{
    int egid = EXTENT_TYPE_TO_GROUPID(egRelNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];
    return eg_df_exists(seg);
}


/*
 * If the space allocates no extent.
 */
static bool spc_empty(SegSpace *spc)
{
    bool ret = true;

    /* check all extent groups */
    for (int i = 0; i < (int)EXTENT_GROUPS; i++) {
        for (int j = 0; j <= SEGMENT_MAX_FORKNUM; j++) {
            SegExtentGroup *eg = &spc->extent_group[i][j];
            ret &= eg_empty(eg);
        }
    }

    return ret;
}

SpaceDataFileStatus spc_status(SegSpace *spc)
{
    bool hasEmpty = false;
    bool hasNormal = false;
    bool hasCrashed = false;
    bool metaNormal = false;

    for (int i = EXTENT_1; i <= EXTENT_8192; i++) {
        int egid = EXTENT_TYPE_TO_GROUPID(i);
        for (int j = 0; j <= SEGMENT_MAX_FORKNUM; j++) {
            SegExtentGroup *seg = &spc->extent_group[egid][j];

            SpaceDataFileStatus egstatus = eg_status(seg);
            if (egstatus == CRASHED) {
                // any extent group crashed, means the whole space is crashed.
                hasCrashed = true;
            }
            if (egstatus == EMPTY) {
                hasEmpty = true;
            }
            if (egstatus == NORMAL) {
                hasNormal = true;
                if (i == EXTENT_1 && j == MAIN_FORKNUM) {
                    metaNormal = true;
                }
            }
        }
    }

    if (hasCrashed) {
        return CRASHED;
    }

    /* Metadata extent group (filename: 1) must exists */
    if (hasNormal && metaNormal) {
        return NORMAL;
    }
    return EMPTY;
}

static pthread_mutex_t segspace_lock = PTHREAD_MUTEX_INITIALIZER;

/* callers must hold the segspace_lock */
void InitSpaceNode(SegSpace *spc, Oid spcNode, Oid dbNode, bool is_redo)
{
    errno_t er = memset_s(spc, sizeof(SegSpace), 0, sizeof(SegSpace));
    pthread_mutex_init(&spc->lock, NULL);

    spc->spcNode = spcNode;
    spc->dbNode = dbNode;
    spc->status = INITIAL;

    securec_check(er, "", "");

    for (int egid = 0; egid < EXTENT_GROUPS; egid++) {
        for (int forknum = 0; forknum <= SEGMENT_MAX_FORKNUM; forknum++) {
            eg_ctrl_init(spc, &spc->extent_group[egid][forknum], EXTENT_GROUPID_TO_SIZE(egid), forknum);
        }
    }

    if (SS_STANDBY_MODE) {
        SSInitSegLogicFile(spc);
    }
}

void spc_clean_extent_groups(SegSpace *spc)
{
    for (int egid = 0; egid < EXTENT_TYPES; egid++) {
        for (int j = 0; j <= SEGMENT_MAX_FORKNUM; j++) {
            SegExtentGroup *eg = &spc->extent_group[egid][j];
            eg_clean_data_files(eg);
        }
    }
}

SegSpace *spc_init_space_node(Oid spcNode, Oid dbNode)
{
    SegSpace *entry = NULL;
    AutoMutexLock spc_lock(&segspace_lock);
    bool found;
    SegSpcTag tag = {.spcNode = spcNode, .dbNode = dbNode};
    SegmentCheck(t_thrd.storage_cxt.SegSpcCache != NULL);

    spc_lock.lock();

    entry = (SegSpace *)hash_search(t_thrd.storage_cxt.SegSpcCache, (void *)&tag, HASH_ENTER, &found);
    if (!found) {
        InitSpaceNode(entry, spcNode, dbNode, false);
    }
    spc_lock.unLock();

    return entry;
}

/*
 * Each pair of <tablespace, database> has only one segment space.
 * This function just creates an object in memory. It does not create
 * any physical files. But it ensures the tablespace directory exsiting.
 */
SegSpace *spc_open(Oid spcNode, Oid dbNode, bool create, bool isRedo)
{
    SegSpace *entry = spc_init_space_node(spcNode, dbNode);

    if (entry->status != OPENED) {
        SpaceDataFileStatus status = spc_status(entry);
        if (status == SpaceDataFileStatus::EMPTY) {
            if (create) {
                /*
                * make sure the directory exists; do it before locking to avoid deadlock with spc_drop
                */
                TablespaceCreateDbspace(entry->spcNode, entry->dbNode, isRedo);
            } else {
                return NULL;
            }
        }
        AutoMutexLock spc_lock(&entry->lock);
        spc_lock.lock();
        entry->status = OPENED;
    }
    return entry;
}

void spc_drop_space_node(Oid spcNode, Oid dbNode)
{
    if (ENABLE_DMS && SS_PRIMARY_MODE) {
        SSBCastDropSegSpace(spcNode, dbNode);
    }

    SegSpace *spc = spc_init_space_node(spcNode, dbNode);
    SegSpcTag tag = {.spcNode = spcNode, .dbNode = dbNode};
    bool found = false;
    AutoMutexLock spc_lock(&spc->lock);
    spc_lock.lock();

    SpaceDataFileStatus dataStatus = spc_status(spc);
    if (dataStatus != SpaceDataFileStatus::EMPTY) {
        spc_clean_extent_groups(spc);
        spc_lock.unLock();
        AutoMutexLock spc_lock(&segspace_lock);
        spc_lock.lock();
        (void)hash_search(t_thrd.storage_cxt.SegSpcCache, (void *)&tag, HASH_REMOVE, &found);
        SegmentCheck(found);
    }
}

/*
 * Check whether the space is empty, if so, drop all metadata buffers.
 *
 * Space object in hash table is not removed. Just set as closed.
 */
SegSpace *spc_drop(Oid spcNode, Oid dbNode, bool redo)
{
    if (ENABLE_DMS && SS_PRIMARY_MODE) {
        SSBCastDropSegSpace(spcNode, dbNode);
    }

    SegSpace *spc = spc_init_space_node(spcNode, dbNode);
    AutoMutexLock spc_lock(&spc->lock);
    spc_lock.lock();

    SpaceDataFileStatus dataStatus = spc_status(spc);
    if (dataStatus == SpaceDataFileStatus::EMPTY) {
        // there is no segment data file, return directly.
        return spc;
    }

    spc_lock.unLock();
    /* Force a checkpoint so that previous xlog won't access this space after recovery. */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
    spc_lock.lock();

    /* Data files are healthy. We can drop the space and remove files if it stores no segment. */
    if (redo || spc_empty(spc)) {
        if (!redo) {
            START_CRIT_SECTION();
            /* Checkpoint is delayed until all works are done */
            t_thrd.pgxact->delayChkpt = true;
            /* We need an xlog for drop tablespace, to drop invalid page when recovery. */
            XLogBeginInsert();
            XLogRegisterData((char *)&spcNode, sizeof(Oid));
            XLogRegisterData((char *)&dbNode, sizeof(Oid));
            XLogRecPtr lsn = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_SPACE_DROP);
            END_CRIT_SECTION();

            XLogWaitFlush(lsn);
        }
        /*
         * We need to scan all buffers to drop buffers; if there are too many databases, it may be slow.
         * Then we should handle all databases of the tablespace through one pass, i.e., the second parameter
         * of this function should be a list of database id.
         */
        SegDropSpaceMetaBuffers(spcNode, dbNode);
        forget_space_fsync_request(spc);
        spc_clean_extent_groups(spc);

        ereport(LOG, (errmsg("drop segment space %u/%u", spcNode, dbNode)));
        if (!redo) {
            t_thrd.pgxact->delayChkpt = false;
        }
    } else {
        /* Even redo is true, the space should be empty. */
        ereport(ERROR, (errmsg("could not remove the tablespace, because there are data in segment files.")));
    }

    return spc;
}

static void SSClose_seg_files(SegSpace *spc)
{
    for (int egid = 0; egid < EXTENT_TYPES; egid++) {
        for (int j = 0; j <= SEGMENT_MAX_FORKNUM; j++) {
            SegExtentGroup *eg = &spc->extent_group[egid][j];
            SegLogicFile *sf = eg->segfile;
            AutoMutexLock filelock(&sf->filelock);
            filelock.lock();

            for (int i = sf->file_num - 1; i >= 0; i--) {
                (void)close(sf->segfiles[i].fd);
                sf->segfiles[i].fd = -1;
                sf->file_num--;
            }
            sf->file_num = 0;
            filelock.unLock();
        }
    }
}

void SSDrop_seg_space(Oid spcNode, Oid dbNode)
{
    SegSpace *entry = NULL;
    AutoMutexLock spc_lock(&segspace_lock);
    SegSpcTag tag = {.spcNode = spcNode, .dbNode = dbNode};
    SegmentCheck(t_thrd.storage_cxt.SegSpcCache != NULL);
    spc_lock.lock();
    entry = (SegSpace *)hash_search(t_thrd.storage_cxt.SegSpcCache, (void *)&tag, HASH_FIND, NULL);
    spc_lock.unLock();

    if (entry != NULL) {
        if(entry->status == OPENED) {
            SSClose_seg_files(entry);
            SegDropSpaceMetaBuffers(spcNode, dbNode);
        }
        spc_lock.lock();
        (void)hash_search(t_thrd.storage_cxt.SegSpcCache, (void *)&tag, HASH_REMOVE, NULL);
    }
    return;
}

/*
 * After shrink, the space's physical size:
 *  1. is aligned to DF_FILE_EXTEND_STEP_BLOCKS (128MB)
 *  2. contains at least DF_FILE_EXTEND_STEP_BLOCKS free space to be used for ongoing allocation requests.
 */
void get_space_shrink_target(SegExtentGroup *seg, BlockNumber *target_size, int *old_group_count, int *new_group_count)
{
    Buffer buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, seg->map_head_entry, RBM_NORMAL);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetBlock(buffer));

    BlockNumber extents = map_head->allocated_extents;
    *old_group_count = map_head->group_count;
    SegUnlockReleaseBuffer(buffer);

    BlockNumber total_data_blocks = extents * seg->extent_size;
    total_data_blocks += DF_FILE_EXTEND_STEP_BLOCKS; // contains extra 128 MB data

    /* First map group */
    BlockNumber meta_blocks = DEFAULT_META_BLOCKS;
    *new_group_count = 1;
    BlockNumber group_blocks = seg->extent_size * DF_MAP_GROUP_SIZE * DF_MAP_BIT_CNT;

    BlockNumber remaining_data = total_data_blocks;
    while (remaining_data > group_blocks) {
        /* If current map group can not contain the rest data blocks, we need more map group */
        remaining_data -= group_blocks;
        (*new_group_count)++;
        meta_blocks += DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE;
    }

    *target_size = total_data_blocks + meta_blocks;
    *target_size = CM_ALIGN_ANY(*target_size, DF_FILE_EXTEND_STEP_BLOCKS);

    ereport(INFO, (errmsg("current blocks: %u, target file blocks: %u, old group count: %d, new group count: %d",
                          seg->segfile->total_blocks, *target_size, *old_group_count, *new_group_count)));
}

/*
 * If buffer does not exist, return InvalidBuffer. It's OK if another backend threads allocating the buffer
 * concurrently, because it will be blocked by segment head when it try to read content from file. Thus
 * InvalidBuffer is returned if BM_VALID is false.
 */
Buffer try_get_moved_pagebuf(RelFileNode *rnode, int forknum, BlockNumber logic_blocknum)
{
    BufferTag tag;
    INIT_BUFFERTAG(tag, *rnode, forknum, logic_blocknum);

    uint32 hashcode = BufTableHashCode(&tag);
    int buf_id = BufTableLookup(&tag, hashcode);
    if (buf_id >= 0) {
        BufferDesc *buf = GetBufferDescriptor(buf_id);

        ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

        /* Pin the buffer to avoid invalidated by others */
        bool valid = PinBuffer(buf, NULL);

        if (!BUFFERTAGS_PTR_EQUAL(&buf->tag, &tag)) {
            UnpinBuffer(buf, true);
            return InvalidBuffer;
        }

        if (!valid) {
            UnpinBuffer(buf, true);
            return InvalidBuffer;
        }

        return BufferDescriptorGetBuffer(buf);
    }

    return InvalidBuffer;
}

/*
 * logic_rnode and logic_start_blocknum is used to get blocks' buffer if possible
 */
void copy_extent(SegExtentGroup* seg, RelFileNode logic_rnode, uint32 logic_start_blocknum, BlockNumber nblocks,
                 BlockNumber phy_from_extent, BlockNumber phy_to_extent, uint32 copy_logic_start_blocknum,
                 ForkNumber forknum)
{
    char *content = NULL;
    char *unaligned_content = NULL;
    BlockNumber copy_logic_blknum = InvalidBlockNumber;
    bool compress = IS_SEG_COMPRESSED_RNODE(logic_rnode, forknum);
    if (ENABLE_DSS) {
        unaligned_content = (char*)palloc(BLCKSZ + ALIGNOF_BUFFER);
        content = (char*)BUFFERALIGN(unaligned_content);
    } else {
        content = (char *)palloc(BLCKSZ);
    }

    char *pagedata = NULL;
    for (int i = 0; i < seg->extent_size; i++) {
        /*
         * If this extent is the last one in the segment, some blocks may be not used (extended) yet.
         * Skip them, otherwise redo XLOG_HEAP_NEWPAGE xlog will generate a buffer whose block number
         * is larger than nblocks of the relation. Once redo finished, and this segment needs to extend
         * a new page, "ReadBuffer" function will find the new extended blocks has already been in the
         * buffer pool and the page is not "new page", which violate the assumption of ReadBuffer. See
         * more details in "ReadBuffer_common".
         */
        if (logic_start_blocknum + i >= nblocks) {
            return;
        }

        if (compress && ((i + 1) % CFS_EXTENT_SIZE == 0)) {
            ereport(LOG, (errmsg("[segment page]we need not to copy pca page,"
                                 "logic_start_blocknum:%d, offset:%d",
                                 logic_start_blocknum, i)));
            continue;
        }
        /*
         * In the extent to be moved, there may exist some blocks cached in shared buffer. Thus we need to
         * copy content from buffer instead of file. Note that we can not use 'ReadBuffer' interface directly,
         * because if buffer does not exist, it will try to allocate a new buffer and invoking smgrread that
         * requires segment head lock. However, segment head lock has been held already here. Invoking ReadBuffer
         * leads to dead lock.
         *
         */
        SMgrRelation rel = smgropen(logic_rnode, InvalidBackendId, GetColumnNum(forknum));
        SMgrOpenSpace(rel);
        SegmentCheck(rel->seg_space != NULL);
        Buffer buf = try_get_moved_pagebuf(&logic_rnode, seg->forknum, logic_start_blocknum + i);
        if (BufferIsValid(buf)) {
            /*
             * Don't worry dead lock. Once buffer is allocated, it won't get segment head lock anymore. FlushBuffer
             * will use physical location and invoke seg_physical_write directly.
             */
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            pagedata = BufferGetPage(buf);

            /*
             * Before we change the physical location in the buffer, we should flush the content to the old location on
             * the disk. Otherwise later checkpoint will flush data to the new physical location and the old block loses
             * the recent modification. If system restart here, the XLogs just before data movement still use the old
             * physical location, they will find data on disk are too old, incurring LSN check failing.
             */
            BufferDesc *buf_desc = BufferGetBufferDescriptor(buf);
            uint64 buf_state = LockBufHdr(buf_desc);
            UnlockBufHdr(buf_desc, buf_state);
            if (buf_state & BM_DIRTY) {
                FlushOneBufferIncludeDW(buf_desc);
                ereport(DEBUG1, (errmodule(MOD_SEGMENT_PAGE),
                                 errmsg("[COPY_EXTENT] buffer is dirty, need flush, logic_start_blocknum:%d, "
                                        "offset:%d, extent_size:%u, phy_from_extent:%u, phy_to_extent:%u",
                                        logic_start_blocknum, i, seg->extent_size, phy_from_extent, phy_to_extent)));
            }
        } else {
            BlockNumber from_block = phy_from_extent + i;
            spc_read_block(seg->space, EXTENT_GROUP_RNODE(seg->space, (ExtentSize)seg->extent_size, logic_rnode.opt),
                           forknum, content, from_block);
            pagedata = content;
        }

        BlockNumber to_block = phy_to_extent + i;

        if (copy_logic_start_blocknum != InvalidBlockNumber) {
            copy_logic_blknum =  copy_logic_start_blocknum + i;
        }

        START_CRIT_SECTION();
        {
            BufferTag tag = {
                .rnode = seg->rnode,
                .forkNum = seg->forknum,
                .blockNum = to_block
            };

            XLogCopyExtent xlog_data;
            xlog_data.tag = tag;
            xlog_data.copy_logic_rnode = logic_rnode;
            xlog_data.copy_logic_blknum = copy_logic_blknum;
            /* 1. xlog insert and set lsn */
            XLogBeginInsert();
            XLogRegisterData((char *)&xlog_data, sizeof(xlog_data));
            XLogRegisterData(pagedata, BLCKSZ);
            XLogRecPtr recptr = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_NEW_PAGE);
            PageSetLSN(pagedata, recptr);

            /* 2. double write */
            if (dw_enabled() && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0) {
                bool flush_old_file = false;
                uint16 pos = seg_dw_single_flush_without_buffer(tag, (Block)pagedata, &flush_old_file);
                t_thrd.proc->dw_pos = pos;
                t_thrd.proc->flush_new_dw = !flush_old_file;
                PageSetChecksumInplace((Page)pagedata, to_block);
                spc_write_block(rel->seg_space,
                                EXTENT_GROUP_RNODE(seg->space, (ExtentSize)seg->extent_size,
                                                   logic_rnode.opt), forknum, pagedata, to_block);

                if (flush_old_file) {
                    g_instance.dw_single_cxt.recovery_buf.single_flush_state[pos] = true;
                } else {
                    g_instance.dw_single_cxt.single_flush_state[pos] = true;
                }
                t_thrd.proc->dw_pos = -1;
            } else {
                PageSetChecksumInplace((Page)pagedata, to_block);
                spc_write_block(rel->seg_space, EXTENT_GROUP_RNODE(seg->space, (ExtentSize)seg->extent_size,
                                                                   logic_rnode.opt),
                                forknum, pagedata, to_block);
            }
        }
        END_CRIT_SECTION();

        SEGMENTTEST(SEGMENT_COPY_BLOCK, (errmsg("error happens just after copy one block")));

        if (BufferIsValid(buf)) {
            BufferDesc *bufdesc = GetBufferDescriptor(buf - 1);
            SegmentCheck(bufdesc->extra->seg_fileno == seg->rnode.relNode);
            bufdesc->extra->seg_blockno = to_block;

            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            UnpinBuffer(bufdesc, true);
        }
    }

    if (ENABLE_DSS) {
        pfree(unaligned_content);
    } else {
        pfree(content);
    }
}

/*
 * Given a segment head block number, return RelFileNode and fork number it represents.
 * aim_fork is used to check data consistency.
 *
 * It is possible that the segment head is freed and reused between we getting its location
 * and locking the segment head. So if the segment is reused, we return a invalid RelFileNode
 * whose relNode = InvalidBlockNumber.
 */
RelFileNode get_segment_logic_rnode(SegSpace *spc, BlockNumber head_blocknum, int aim_fork)
{
    /* segment head must be located at metadata extent group */
    SegExtentGroup* seg = &spc->extent_group[EXTENT_TYPE_TO_GROUPID(SEGMENT_HEAD_EXTENT_TYPE)][MAIN_FORKNUM];
    Buffer ipbuf = InvalidBuffer;

    ExtentInversePointer iptr = GetInversePointer(seg, head_blocknum, &ipbuf);
    ExtentUsageType usage = SPC_INVRSPTR_GET_USAGE(iptr);

    RelFileNode rnode = seg->rnode;
    int forknum = MAIN_FORKNUM;

    if (usage == ExtentUsageType::SEGMENT_HEAD) {
        rnode.relNode = head_blocknum;
        rnode.bucketNode = SegmentBktId;
        SegmentCheck(iptr.owner == InvalidBlockNumber);
    } else if (usage == ExtentUsageType::FORK_HEAD) {
        forknum = (int)SPC_INVRSPTR_GET_SPECIAL_DATA(iptr);
        SegmentCheck(forknum != MAIN_FORKNUM);
        rnode.relNode = iptr.owner;
        rnode.bucketNode = SegmentBktId;
    } else if (usage == ExtentUsageType::BUCKET_HEAD) {
        uint32 special_data = SPC_INVRSPTR_GET_SPECIAL_DATA(iptr);
        forknum = special_data / MAX_BUCKETMAPLEN;
        rnode.bucketNode = special_data % MAX_BUCKETMAPLEN;
        rnode.relNode = iptr.owner;
    } else {
        rnode.relNode = InvalidBlockNumber;
        ereport(LOG,
            (errmodule(MOD_SEGMENT_PAGE),
                errmsg("segment head block %u, but extent usage is %u, (iptr: %u/%u)",
                    head_blocknum,
                    usage,
                    iptr.owner,
                    iptr.flag)));
    }

    if (aim_fork != forknum) {
        /* Forknumber is not matched. */
        rnode.relNode = InvalidBlockNumber;
    }

    if (BufferIsValid(ipbuf)) {
        SegReleaseBuffer(ipbuf);
    }

    return rnode;
}

Oid get_relation_oid(Oid spcNode, Oid relNode)
{
    Oid toastid = InvalidOid;
    Oid relation_oid = HeapGetRelid(spcNode, relNode, toastid, NULL, true);
    return relation_oid;
}

/*
 * same as get_relation_oid except we check for cache invalidation here;
 * If relation oid is valid, lock it before return.
 */
Oid get_valid_relation_oid(Oid spcNode, Oid relNode)
{
    Oid reloid, oldreloid;
    bool retry = false;
    uint64 sess_inval_count;
    uint64 thrd_inval_count = 0;
    for (;;) {
        sess_inval_count = u_sess->inval_cxt.SIMCounter;
        if (EnableLocalSysCache()) {
            thrd_inval_count = t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter;
        }
        reloid = get_relation_oid(spcNode, relNode);

        if (retry) {
            /* nothing changed after we lock the relation */
            if (reloid == oldreloid) {
                return reloid;
            }
            if (OidIsValid(oldreloid)) {
                UnlockRelationOid(oldreloid, AccessExclusiveLock);
            }
            /* relation oid is invalid after retry */
            if (!OidIsValid(reloid)) {
                return InvalidOid;
            }
        }

        if (OidIsValid(reloid)) {
            LockRelationOid(reloid, AccessExclusiveLock);
        }

        /* No invalidation message */
        if (EnableLocalSysCache()) {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter &&
                thrd_inval_count == t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter) {
                return reloid;
            }
        } else {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter) {
                return reloid;
            }
        }
        retry = true;
        oldreloid = reloid;
    }
}

void move_data_extent(SegExtentGroup *seg, BlockNumber extent, ExtentInversePointer iptr, ForkNumber forknum)
{
    BlockNumber logic_start;
    uint32 extent_id = SPC_INVRSPTR_GET_SPECIAL_DATA(iptr);
    BlockNumber owner = iptr.owner;

    SegSpace *spc = seg->space;
    Oid spcNode = spc->spcNode;
    Oid dbNode = spc->dbNode;

    RelFileNode logic_rnode = get_segment_logic_rnode(spc, owner, seg->forknum);

    Oid relation_oid = get_valid_relation_oid(spcNode, logic_rnode.relNode);
    if (!OidIsValid(relation_oid)) {
        /*
         * As we lock the database to prevent any DDL, current segment should have a 'owner' relation or partition.
         * But it's OK just to skip moving this extent.
         */
        ereport(WARNING,
                (errmsg("RelFileNode <%u/%u/%u> does not find a matching relation in pg_class and pg_partition.",
                        logic_rnode.spcNode, logic_rnode.dbNode, logic_rnode.spcNode)));
        return;
    }
    SMgrRelation rel = smgropen(logic_rnode, InvalidBackendId, GetColumnNum(forknum));
    logic_rnode.opt = rel->smgr_rnode.node.opt;
    /*
     * Lock the segment head buffer first. So concurrent workers can not
     * (1) modify the block map tree (2) free the segment
     * And we should check the segment head and extent again, to ensure the extent can be moved.
     */
    LockSegmentHeadPartition(spcNode, dbNode, owner, LW_EXCLUSIVE);

    /* Note that segment head is always in EXTENT_1, and forknum is MAIN_FORK */
    RelFileNode fakenode = seg->rnode;
    fakenode.relNode = SEGMENT_HEAD_EXTENT_TYPE;

    Buffer buffer = ReadBufferFast(spc, fakenode, MAIN_FORKNUM, owner, RBM_NORMAL);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    SegmentHead *owner_seghead = (SegmentHead *)PageGetContents(BufferGetBlock(buffer));

    bool need_move = true;
    if (BlockNumberIsValid(logic_rnode.relNode)) {
        if (owner_seghead->nextents <= extent_id) {
            need_move = false;
        } else {
            BlockNumber curr_extent = seg_extent_location(spc, owner_seghead, extent_id);
            if (curr_extent != extent) {
                ereport(LOG,
                    (errmsg("extent: %u, segment head: %u, forknumber: %d, extent id: %u, but current extent is %u",
                        extent,
                        owner,
                        seg->forknum,
                        extent_id,
                        curr_extent)));
                need_move = false;
            }
        }
    } else {
        need_move = false;
    }

    if (!need_move) {
        SegUnlockReleaseBuffer(buffer);
        UnlockSegmentHeadPartition(spcNode, dbNode, owner);
        UnlockRelationOid(relation_oid, AccessExclusiveLock);
        return;
    }

    /* Check is passed. Now we can do the data movement */
    if (!IS_SEG_COMPRESSED_RNODE(EXTENT_GROUP_RNODE(seg->space, (ExtentSize)seg->extent_size, logic_rnode.opt),
                                 seg->forknum)) {
        logic_start = ExtentIdToLogicBlockNum(extent_id);
    } else {
        logic_start = ExtentIdToLogicBlocknumInCfs(extent_id);
    }

    XLogAtomicOpStart();

    /* Inverse pointer for the new extent is the same as the old extent */
    BlockNumber new_extent = eg_alloc_extent(seg, InvalidBlockNumber, iptr);
    SegmentCheck(new_extent < extent);

    /* eg_alloc_extent implicate START_CRIT_SECTION; We do not worry any Error during copy_extent */
    copy_extent(seg, logic_rnode, logic_start, owner_seghead->nblocks, extent, new_extent, InvalidBlockNumber, forknum);

    SEGMENTTEST(SEGMENT_COPY_EXTENT, (errmsg("error happens just after copy extent")));

    /* Update Block Map Tree */
    if (extent_id < BMT_HEADER_LEVEL0_SLOTS) {
        owner_seghead->level0_slots[extent_id] = new_extent;
    } else {
        seg_record_new_extent_on_level0_page(spc, buffer, extent_id, new_extent);
    }

    /* Xlog issue; even if the extent located in level0 page, we still need the xlog to update buffer
     * descriptors when redo. */
    XLogMoveExtent xlog_data;
    xlog_data.logic_rnode = logic_rnode;
    xlog_data.forknum = seg->forknum;
    xlog_data.nblocks = owner_seghead->nblocks;
    xlog_data.extent_id = extent_id;
    xlog_data.new_extent = new_extent;
    xlog_data.old_extent = extent;

    XLogAtomicOpRegisterBuffer(buffer, REGBUF_KEEP_DATA, SPCXLOG_SHRINK_SEGHEAD_UPDATE, XLOG_COMMIT_KEEP_BUFFER_STATE);
    XLogAtomicOpRegisterBufData((char *)&xlog_data, sizeof(xlog_data));
    XLogAtomicOpCommit();

    SegUnlockReleaseBuffer(buffer);

    UnlockSegmentHeadPartition(spcNode, dbNode, owner);
    UnlockRelationOid(relation_oid, AccessExclusiveLock);

    SEGMENTTEST(SEGMENT_COPY_UPDATE_SEGHEAD, (errmsg("error happens just after updating segment head when shrink")));

    /*
     * eg_free_extent is included in an XLogAtomicOperation alone, because eg_alloc_extent and eg_free_extent
     * will acquire exclusive lock for the same buffer. But in an XLogAtomicOperation, once the buffer is
     * locked, it is unlocked until xlog committed. Thus, eg_free_extent can not be in the same xlog atomic
     * operation with eg_alloc_extent, otherwise this thread reuires exclusive lock twice on the same LWLock.
     *
     * Note extent leak may happens if system crashes before eg_free_extent, i.e., the old extent is not recycled.
     * The special clean-up procedure will handle this situation.
     */
    XLogAtomicOpStart();
    eg_free_extent(seg, extent);
    XLogAtomicOpCommit();
    ereport(DEBUG1, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("[MOVE_EXTENT] moved extent %u to new location %u, owner %u, fork number %d, extent id %u",
                            extent, new_extent, owner, seg->forknum, extent_id)));
}

void move_one_extent(SegExtentGroup *seg, BlockNumber extent, Buffer *ipbuf, ForkNumber forknum)
{
    ExtentInversePointer iptr = GetInversePointer(seg, extent, ipbuf);
    ExtentUsageType usage = SPC_INVRSPTR_GET_USAGE(iptr);

    if (usage == DATA_EXTENT) {
        move_data_extent(seg, extent, iptr, forknum);
    } else {
        uint32 usage = SPC_INVRSPTR_GET_USAGE(iptr);
        uint32 special = SPC_INVRSPTR_GET_SPECIAL_DATA(iptr);
        ereport(PANIC,
                (errmsg("We can not shrink ExtentGroups except data extents. Inverse pointer may be corrupted"),
                 errdetail("Extent group %u/%u/%d, inverse point usage type %u (%s), owner: %u, special_data: %u",
                           seg->rnode.spcNode, seg->rnode.dbNode, seg->extent_size, usage, GetExtentUsageName(iptr),
                           iptr.owner, special)));
    }
}

struct ShrinkVictimSelector {
  public:
    SegExtentGroup *seg;
    df_map_group_t groups[DF_MAX_MAP_GROUP_CNT];
    int group_count;
    BlockNumber target_size;

    int last_group;
    BlockNumber last_map;
    uint16 last_bit;
    bool compact_search;

    void init(SegExtentGroup *seg, BlockNumber target_size, bool compact_search = false);
    BlockNumber next();
};

void ShrinkVictimSelector::init(SegExtentGroup *seg, BlockNumber target_size, bool compact_search)
{
    this->target_size = target_size;
    this->compact_search = compact_search;

    Buffer buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, seg->map_head_entry, RBM_NORMAL);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetBlock(buffer));

    this->group_count = map_head->group_count;
    for (int i = 0; i < group_count; i++) {
        this->groups[i] = map_head->groups[i];
    }
    SegUnlockReleaseBuffer(buffer);

    this->last_group = this->group_count - 1;
    df_map_group_t group = this->groups[this->last_group];
    this->last_map = group.first_map + group.page_count - 1;
    this->last_bit = DF_MAP_BIT_CNT;
    this->seg = seg;
}

BlockNumber ShrinkVictimSelector::next()
{
    // from back to front
    for (; this->last_group >= 0; this->last_group--) { // each group
        df_map_group_t group = this->groups[this->last_group];
        for (; this->last_map >= group.first_map; this->last_map--) { // each map block
            Buffer map_buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, last_map, RBM_NORMAL);
            LockBuffer(map_buffer, BUFFER_LOCK_SHARE);

            df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetBlock(map_buffer));

            if (this->last_bit > map_page->dirty_last + 1) {
                this->last_bit = map_page->dirty_last + 1;
            }

            for (; this->last_bit > 0; this->last_bit--) { // each extent
                uint16 p = this->last_bit - 1;
                BlockNumber extent = map_page->first_page + p * this->seg->extent_size;
                if (!this->compact_search && (extent + seg->extent_size <= this->target_size)) {
                    SegUnlockReleaseBuffer(map_buffer);
                    return InvalidBlockNumber;
                }

                if (DF_MAP_NOT_FREE(map_page->bitmap, p)) {
                    ereport(DEBUG5,
                        (errmodule(MOD_SEGMENT_PAGE),
                            errmsg("group %d, map %u, offset %u needs move", this->last_group, this->last_map, p)));
                    this->last_bit--;
                    SegUnlockReleaseBuffer(map_buffer);
                    return extent;
                }
            }
            SegUnlockReleaseBuffer(map_buffer);

            // amend this->last_bit at the end of the loop
            SegmentCheck(this->last_bit == 0);
            this->last_bit = DF_MAP_BIT_CNT;
        }

        // amend this->last_map at the end of the loop
        if (this->last_group > 0) {
            SegmentCheck(this->last_map < group.first_map);
            group = this->groups[this->last_group - 1];
            this->last_map = group.first_map + group.page_count - 1;
        }
    }
    return InvalidBlockNumber;
}

/* Move extents which is behind targetSize */
void move_extents(SegExtentGroup *seg, BlockNumber target_size, ForkNumber forknum)
{
    /*
     * Copy meta-data from map head, and release the buffer.
     * Each time, we (1) select one extent (2) move it. Step (1) and (2) require locks independently to avoid deadlock.
     */
    ShrinkVictimSelector selector;
    selector.init(seg, target_size);

    Buffer ipbuf = InvalidBuffer;

    BlockNumber victim = selector.next();
    while (victim != InvalidBlockNumber) {
        CHECK_FOR_INTERRUPTS();
        move_one_extent(seg, victim, &ipbuf, forknum);
        victim = selector.next();
    }

    if (BufferIsValid(ipbuf)) {
        SegReleaseBuffer(ipbuf);
    }
}

/*
 * Shrink high water marker
 *
 * Return whether the hwm is updated.
 */
BlockNumber shrink_hwm(SegExtentGroup *seg, BlockNumber target_size)
{
    SegmentCheck((target_size % DF_FILE_EXTEND_STEP_BLOCKS) == 0);
    Buffer buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, seg->map_head_entry, RBM_NORMAL);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetBlock(buffer));

    BlockNumber hwm = map_head->high_water_mark;
    BlockNumber new_hwm = hwm;
    bool end = false;
    int i = map_head->group_count - 1;
    uint16 new_count = map_head->group_count;
    for (; !end && i >= 0; i--) {
        new_count = i + 1;
        BlockNumber first_map_block = map_head->groups[i].first_map;
        for (int j = map_head->groups[i].page_count - 1; !end && j >= 0; j--) {
            BlockNumber map_block = first_map_block + j;

            Buffer map_buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, map_block, RBM_NORMAL);
            LockBuffer(map_buffer, BUFFER_LOCK_SHARE);
            df_map_page_t *map_page = (df_map_page_t *)PageGetContents(BufferGetBlock(map_buffer));

            if (map_page->first_page >= hwm) {
                // extents managed by this map block must be un-allocated, because they exceed the range of hwm.
                SegUnlockReleaseBuffer(map_buffer);
                continue;
            }

            int last_bit = map_page->dirty_last;
            for (; last_bit >= 0; last_bit--) {
                BlockNumber extent = map_page->first_page + seg->extent_size * last_bit;

                if (extent + seg->extent_size <= target_size) {
                    // this extent is in the range of target size, we can not shrink the hwm anymore.
                    end = true;
                    break;
                }
                if (extent >= hwm) {
                    SegmentCheck(DF_MAP_FREE(map_page->bitmap, last_bit));
                    continue;
                }

                if (DF_MAP_FREE(map_page->bitmap, last_bit)) {
                    new_hwm = extent;
                } else {
                    // the extent is used, we can not shrink the hwm anymore.
                    end = true;
                    break;
                }
            }

            SegUnlockReleaseBuffer(map_buffer);
        }
    }

    /* new high water mark is aligned to file extend step. */
    new_hwm = CM_ALIGN_ANY(new_hwm, DF_FILE_EXTEND_STEP_BLOCKS);
    if (new_hwm < target_size) {
        new_hwm = target_size;
    }

    if (new_hwm < hwm) {
        SegmentCheck(new_count <= map_head->group_count);
        ereport(LOG, (errmsg("Update high water mark successfully. Space (%u %u %u), high water mark from %u to %u, "
                             "group count from %u to %u",
                         seg->rnode.spcNode, seg->rnode.dbNode, seg->rnode.relNode, hwm, new_hwm, map_head->group_count,
                         new_count)));

        START_CRIT_SECTION();
        XLogAtomicOpStart();
        XLogDataUpdateSpaceHWM xlog_data;
        xlog_data.new_hwm = new_hwm;
        xlog_data.old_hwm = hwm;
        xlog_data.old_groupcnt = map_head->group_count;
        xlog_data.new_groupcnt = new_count;
        XLogAtomicOpRegisterBuffer(buffer, REGBUF_KEEP_DATA, SPCXLOG_SPACE_UPDATE_HWM, XLOG_COMMIT_KEEP_BUFFER_STATE);
        XLogAtomicOpRegisterBufData((char *)&xlog_data, sizeof(XLogDataUpdateSpaceHWM));
        XLogAtomicOpCommit();
        END_CRIT_SECTION();

        map_head->high_water_mark = new_hwm;
        map_head->group_count = new_count;
        SegUnlockReleaseBuffer(buffer);
        return new_hwm;
    }

    SegUnlockReleaseBuffer(buffer);
    return InvalidBlockNumber;
}

inline static bool IsBufferToBeTruncated(BufferDesc *bufdesc, SegExtentGroup *seg, BlockNumber target_size)
{
    return RelFileNodeEquals(bufdesc->tag.rnode, seg->rnode) && bufdesc->tag.blockNum >= target_size;
}

/*
 * Invalidate meta-data buffer, including MapBlock and InversePointer block buffer.
 */
static void invalidate_metadata_buffer(SegExtentGroup *seg, BlockNumber target_size)
{
    for (int i = SegmentBufferStartID; i < TOTAL_BUFFER_NUM; i++) {
        BufferDesc *bufdesc = GetBufferDescriptor(i);
        uint64 state;

        if (IsBufferToBeTruncated(bufdesc, seg, target_size)) {
            state = LockBufHdr(bufdesc);
            if (IsBufferToBeTruncated(bufdesc, seg, target_size) && (state & BM_DIRTY) && (state & BM_VALID)) {
                InvalidateBuffer(bufdesc); /* release buffer internal */
            } else {
                UnlockBufHdr(bufdesc, state);
            }
        }
    }
}

/* truncate physical data files according to high water mark */
void spc_shrink_files(SegExtentGroup *seg, BlockNumber target_size, bool redo)
{
    /*
     * We must record the xlog before do the actual ftruncate in case of system failure before xlog.
     * Checkpoint should also be delayed to avoid passing the xlog before it has been actually done.
     */
    if (!redo) {
        START_CRIT_SECTION();
        t_thrd.pgxact->delayChkpt = true;
        XLogBeginInsert();
        XLogDataSpaceShrink xlog_data;
        xlog_data.rnode = seg->rnode;
        xlog_data.target_size = target_size;
        xlog_data.forknum = seg->forknum;
        XLogRegisterData((char *)&xlog_data, sizeof(XLogDataSpaceShrink));
        XLogRecPtr lsn = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_SPACE_SHRINK);

        /* Standby's log is reported in "redo_space_shrink" */
        ereport(LOG, (errmsg("call space shrink files, filename: %s, xlog lsn: %lX",
                              relpathperm(seg->rnode, seg->forknum), lsn)));
        END_CRIT_SECTION();

        SEGMENTTEST(SEGMENT_SHRINK_FILE_XLOG, (errmsg("error happens just after shrink xlog")));
        XLogWaitFlush(lsn);
    }

    Buffer buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, seg->map_head_entry, RBM_NORMAL);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    df_map_head_t *map_head = (df_map_head_t *)PageGetContents(BufferGetBlock(buffer));

    BlockNumber hwm = map_head->high_water_mark;
    SegmentCheck((target_size % DF_FILE_EXTEND_STEP_BLOCKS)== 0);

    if (hwm > target_size) {
        /* If in recovery, it is possible data file is extended after this xlog. so just return */
        if (redo) {
            SegUnlockReleaseBuffer(buffer);
            return;
        }
        ereport(PANIC, (errmsg("Segment-page shrink files, <%u %u %u>, target blocks is %u, but high water mark is %u",
                               seg->rnode.spcNode, seg->rnode.dbNode, seg->rnode.relNode, target_size, hwm)));
    }

    /*
     * Must invalidate meta data buffers before shrinking the physical file, otherwise
     * pagewriter and bgwriter flushing dirty buffer will try to access the truncated file.
     *
     * Do not worry about deadlock. Once we lock the space lock, other backends won't
     * lock the metadata buffers, except pagewriter and bgwriter. But flushing buffer will
     * use seg_physical_write that does not acquire the space lock.
     */
    invalidate_metadata_buffer(seg, target_size);

    SEGMENTTEST(SEGMENT_SHRINK_INVALIDATE_BUFFER,
                (errmsg("error happens just after invalidating meta data buffer during shrink")));
    eg_shrink_df(seg, target_size);

    SegUnlockReleaseBuffer(buffer);

    if (!redo) {
        t_thrd.pgxact->delayChkpt = false;
    }
}

void spc_shrink(Oid spcNode, Oid dbNode, int extent_type, ForkNumber forknum)
{
    SegmentCheck(extent_type >= EXTENT_8 && extent_type <= EXTENT_8192);

    SegSpace *spc = spc_open(spcNode, dbNode, false);
    if (spc == NULL) {
        ereport(LOG, (errmsg("Segment is not initialized in current database")));
        return;
    }
    SegExtentGroup *seg = &spc->extent_group[EXTENT_TYPE_TO_GROUPID(extent_type)][forknum];
    if (!eg_df_exists(seg)) {
        ereport(LOG, (errmsg("Segment is not initialized in current database")));
        return;
    }

    BlockNumber target_size = 0;
    int new_group_count, old_group_count;
    get_space_shrink_target(seg, &target_size, &old_group_count, &new_group_count);

    // move extents to the front of data file
    move_extents(seg, target_size, forknum);

    /*
     * We must lock the segment extent group here, to forbid any extent allocation.
     */
    AutoMutexLock spc_lock(&spc->lock);
    spc_lock.lock();

    BlockNumber new_hwm = shrink_hwm(seg, target_size);
    if (BlockNumberIsValid(new_hwm)) {
        spc_shrink_files(seg, new_hwm, false);
    }
}

static int gs_space_shrink_internal(Oid spaceid, Oid dbid, uint32 extent_type, ForkNumber forknum)
{
    if (dbid != u_sess->proc_cxt.MyDatabaseId) {
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errmsg("database id is not current database")));
    }
    if (!ExtentTypeIsValid(extent_type)) {
        ereport(ERROR, (errmsg("The parameter extent_type is not valid"), errhint("extent_type should be in [1, 5]")));
    }
    if (extent_type == 1) {
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("We do not support shrink metadata ExtentGroup yet."),
                        errdetail("Metadata extent_type is 1, data extent_type is 2,3,4,5.")));
    }
    if (forknum < 0 || forknum > MAX_FORKNUM) {
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Invalid fork number."),
                        errdetail("forknum should be in [0, %d]", MAX_FORKNUM)));
    }

    if (forknum > SEGMENT_MAX_FORKNUM) {
        return 0;
    }

    spc_shrink(spaceid, dbid, extent_type, forknum);
    return 0;
}

Datum gs_space_shrink(PG_FUNCTION_ARGS)
{
    if (!XLogInsertAllowed()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Don't shrink space, for recovery is in progress.")));
    }

    if (SS_STANDBY_MODE) {
        ereport(ERROR, (errmsg("SS standby cannot perform gs_space_shrink")));
    }

    Oid spaceid = PG_GETARG_OID(0);
    Oid dbid = PG_GETARG_OID(1);
    uint32 extent_type = PG_GETARG_UINT32(2);
    ForkNumber forknum = PG_GETARG_INT32(3);

    AclResult aclresult = pg_tablespace_aclcheck(spaceid, GetUserId(), ACL_VACUUM);
    if (aclresult != ACLCHECK_OK) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied for tablespace %u, required VACUUM permissions to shrink", spaceid)));
    }

    return gs_space_shrink_internal(spaceid, dbid, extent_type, forknum);
}

Datum local_space_shrink(PG_FUNCTION_ARGS)
{
    if (!XLogInsertAllowed()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Don't shrink space locally, for recovery is in progress.")));
    }

    if (SS_STANDBY_MODE) {
        ereport(ERROR, (errmsg("SS standby cannot perform local_space_shrink")));
    }

    char *tablespacename = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *dbname = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid spaceid = get_tablespace_oid_by_name(tablespacename);
    Oid dbid = get_database_oid_by_name(dbname);

    AclResult aclresult = pg_tablespace_aclcheck(spaceid, GetUserId(), ACL_VACUUM);
    if (aclresult != ACLCHECK_OK) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied for tablespace %s, required VACUUM permissions to shrink", tablespacename)));
    }

    for (int extent_type = EXTENT_8; extent_type <= EXTENT_8192; extent_type++) {
        for (ForkNumber forknum = MAIN_FORKNUM; forknum <= SEGMENT_MAX_FORKNUM; forknum++) {
            gs_space_shrink_internal(spaceid, dbid, extent_type, forknum);
        }
    }

    return 0;
}

Datum global_space_shrink(PG_FUNCTION_ARGS)
{
    if (!XLogInsertAllowed()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Don't shrink space globally, for recovery is in progress.")));
    }

    if (SS_STANDBY_MODE) {
        ereport(ERROR, (errmsg("SS standby cannot perform global_space_shrink")));
    }

    char *tablespacename = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *dbname = text_to_cstring(PG_GETARG_TEXT_PP(1));

    /* check tablespace exiting and ACL before distributing to DNs */
    get_tablespace_oid_by_name(tablespacename);
    Oid dbid = get_database_oid_by_name(dbname);
    if (dbid != u_sess->proc_cxt.MyDatabaseId) {
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errmsg("database id is not current database")));
    }

    /* DDL is forbidden during space shrink */
    DirectFunctionCall2(pg_advisory_xact_lock_int4, t_thrd.postmaster_cxt.xc_lockForBackupKey1,
                        t_thrd.postmaster_cxt.xc_lockForBackupKey2);

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "select pg_catalog.local_space_shrink(\'%s\', \'%s\')", tablespacename, dbname);
    ParallelFunctionState* state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_DATANODES, true);
    FreeParallelFunctionState(state);

    return 0;
}

/*
 * System view
 */
SegmentSpaceStat spc_storage_stat(SegSpace *spc, int group_id, ForkNumber forknum)
{
    SegExtentGroup *eg = &spc->extent_group[group_id][forknum];
    return eg_storage_stat(eg);
}

static void PrintSpaceConstants()
{
    ereport(LOG,
        (errmodule(MOD_SEGMENT_PAGE),
            errmsg("Segment-page constants: DF_MAP_SIZE: %u, DF_MAP_BIT_CNT: %u, DF_MAP_GROUP_EXTENTS: %u, "
                   "IPBLOCK_SIZE: %u, EXTENTS_PER_IPBLOCK: %u, IPBLOCK_GROUP_SIZE: %u, BMT_HEADER_LEVEL0_TOTAL_PAGES: "
                   "%u, BktMapEntryNumberPerBlock: %u, BktMapBlockNumber: %u, BktBitMaxMapCnt: %u",
                DF_MAP_SIZE,
                DF_MAP_BIT_CNT,
                DF_MAP_GROUP_EXTENTS,
                IPBLOCK_SIZE,
                EXTENTS_PER_IPBLOCK,
                IPBLOCK_GROUP_SIZE,
                BMT_HEADER_LEVEL0_TOTAL_PAGES,
                BktMapEntryNumberPerBlock,
                BktMapBlockNumber,
                BktBitMaxMapCnt)));
}

/*
 * SegSpcCreate
 *
 * Create seg spc cache spaces.
 */
void InitSegSpcCache(void)
{
    HASHCTL ctl;

    /* hash accessed by database file id */
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(SegSpcTag); // tablespce_id + databaes_id
    ctl.entrysize = sizeof(SegSpace);
    ctl.hash = tag_hash;

    t_thrd.storage_cxt.SegSpcCache =
        HeapMemInitHash("Shared Seg Spc hash by request", 256, 81920, &ctl, HASH_ELEM | HASH_FUNCTION);
    if (!t_thrd.storage_cxt.SegSpcCache)
        ereport(FATAL, (errmsg("could not initialize shared Seg Spc hash table")));

    if (AmPostmasterProcess()) {
        PrintSpaceConstants();
    }
}

static bool SSCheckIfSegLogicFileNormal(SegExtentGroup *seg)
{
    SegLogicFile *sf = seg->segfile;
    if (sf->total_blocks < DF_FILE_MIN_BLOCKS) {
        return false;
    }

    int fd = BasicOpenFile(sf->filename, O_RDWR | PG_BINARY, S_IWUSR | S_IRUSR);
    if (fd < 0) {
        ereport(ERROR, (errmsg("open_file failed filename: %s, fd is %d, %d", sf->filename, fd, errno)));
    }
    sf->segfiles[0].fd = fd;
    char* buffer = (char *)palloc(BLCKSZ + ALIGNOF_BUFFER);
    char* aligned_buffer = (char *)BUFFERALIGN(buffer);
    int nbytes = pread(fd, aligned_buffer, BLCKSZ, DF_MAP_HEAD_PAGE * BLCKSZ);
    if (nbytes != BLCKSZ) {
        ereport(ERROR, (errmsg("could not read segment meta block in file %s, %d", sf->filename, errno)));
    }

    if (!PageIsVerified((Page)aligned_buffer, DF_MAP_HEAD_PAGE)) {
        pfree(buffer);
        return false;
    }

    df_map_head_t *map_head = (df_map_head_t *)PageGetContents((Page)aligned_buffer);
    if (map_head->bit_unit != seg->extent_size) {
        pfree(buffer);
        return false;
    }

    pfree(buffer);
    return true;
}

static void SSUpdateSegLogicFileSize(SegSpace *spc)
{
    bool is_normal = true;
    bool is_meta_normal = false;

    for (int egid = 0; egid < EXTENT_GROUPS; egid++) {
        for (int forknum = 0; forknum <= SEGMENT_MAX_FORKNUM; forknum++) {
            SegLogicFile *sf = spc->extent_group[egid][forknum].segfile;
            if (sf->file_num == 0) {
                continue;
            }

            struct stat statbuf;
            if (sf->file_num == 1) {
                if (stat(sf->filename, &statbuf) == 0) {
                    sf->total_blocks = statbuf.st_size / BLCKSZ;
                } else {
                    ereport(ERROR, (errmsg("failed stat file %s during init segment file.", sf->filename)));
                }
            } else {
                char fullpath[MAXPGPATH];
                errno_t rc = sprintf_s(fullpath, MAXPGPATH, "%s.%d", sf->filename, sf->file_num - 1);
                securec_check_ss(rc, "\0", "\0");
                if (stat(fullpath, &statbuf) == 0) {
                    sf->total_blocks = statbuf.st_size / BLCKSZ + (sf->file_num - 1) * EXT_SIZE_1024_TOTAL_PAGES;
                } else {
                    ereport(ERROR, (errmsg("failed stat file %s during init segment file.", fullpath)));
                }
            }

            if (!is_normal) {
                continue;
            }

            /* we can set spc status to open here, only need to open sf->filename and read one block to verify */
            if (is_normal && SSCheckIfSegLogicFileNormal(&(spc->extent_group[egid][forknum]))) {
                if (egid == 0 && forknum == 0) {
                    is_meta_normal = true;
                }
            } else {
                is_normal = false;
            }
        }
    }

    if (is_meta_normal && is_normal) {
        spc->status = OPENED;
    }
}

static void SSUpdateSegLogicFileNum(SegLogicFile* sf, char* dirpath, char* filename)
{
    int sliceno = sf->file_num + 1;
    if (sliceno > sf->vector_capacity) {
        df_extend_file_vector(sf);
    }
    sf->segfiles[sf->file_num].sliceno = sf->file_num;
    sf->file_num++;
}

static void SSInitSegLogicFile(SegSpace *spc)
{
    if (spc->extent_group[0][0].segfile == NULL) {
        return;
    }
    SegmentCheck(spc->extent_group[0][0].segfile->filename[0] != '\0');
    /* Get path of dir from seg filename */
    char dirpath[MAXPGPATH];
    int count = strlen(spc->extent_group[0][0].segfile->filename) - SEG_MAINFORK_FILENAME_LEN;
    int rc = EOK;
    rc = strncpy_s(dirpath, MAXPGPATH, spc->extent_group[0][0].segfile->filename, count);
    securec_check_c(rc, "\0", "\0");

    /*
     * Read dir and fill seg logic file except fd.
     * For filenum and total block, we only need to check the filename and size under the dir.
     * For fd, we can construct the filename and open it when we really need use the file.
     */
    DIR *data_dir = NULL;
    struct dirent *data_de = NULL;
    data_dir = opendir(dirpath);
    if (data_dir == NULL) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("could not open data dir %s during init segment file.", dirpath)));
    }

    while ((data_de = readdir(data_dir)) != NULL) {
        if (!isdigit(data_de->d_name[0])) {
            continue;
        }

        char tmp_path[MAXPGPATH];
        int suffix = 0;
        rc = sscanf_s(data_de->d_name, "%[^.].%d", tmp_path, MAXPGPATH, &suffix);
        if (rc <= 0) {
            ereport(LOG, (errmsg("skip %s as it is not segment file.", data_de->d_name)));
            continue;
        }
        int extent_size = tmp_path[0] - '0';
        int tmp_length = strlen(tmp_path);
        if (strstr(tmp_path, "_vm") != NULL && tmp_length == SEG_VMFORK_FILENAME_LEN && extent_size >= EXTENT_1 &&
            extent_size <= EXTENT_8192) {
            SSUpdateSegLogicFileNum(spc->extent_group[extent_size - 1][VISIBILITYMAP_FORKNUM].segfile, dirpath,
                data_de->d_name);
        } else if (strstr(tmp_path, "_fsm") != NULL && tmp_length == SEG_FSMFORK_FILENAME_LEN &&
            extent_size >= EXTENT_1 && extent_size <= EXTENT_8192) {
            SSUpdateSegLogicFileNum(spc->extent_group[extent_size - 1][FSM_FORKNUM].segfile, dirpath, data_de->d_name);
        } else if (tmp_length == 1 && extent_size >= EXTENT_1 && extent_size <= EXTENT_8192) {
            SSUpdateSegLogicFileNum(spc->extent_group[extent_size - 1][MAIN_FORKNUM].segfile, dirpath, data_de->d_name);
        } else {
            ereport(LOG, (errmsg("skip %s as it is not segment file.", data_de->d_name)));
        }
    }
    SSUpdateSegLogicFileSize(spc);
    closedir(data_dir);
}

/**
 * @brief Mark global indexes as unusable for a partitioned table.
 *
 * This function iterates through all indexes of the given partitioned table (Relation),
 * identifies global indexes, and sets their state to unusable. This is typically called
 * after a shrink operation on the partition table which might have invalidated index entries.
 *
 * @param[in] rel The partitioned table relation.
 */
static void rel_shrink_global_idx_unusable(Relation rel)
{
    List* indexList = RelationGetIndexList(rel);
    ListCell* lc = NULL;
    foreach (lc, indexList) {
        Oid indexOid = lfirst_oid(lc);
        Relation indexRel = index_open(indexOid, RowExclusiveLock);
        if (RelationIsGlobalIndex(indexRel)) {
            ATExecSetIndexUsableState(IndexRelationId, indexOid, false);
            CacheInvalidateRelcacheByRelid(indexOid);
        }
        index_close(indexRel, RowExclusiveLock);
    }
    list_free(indexList);
}

/**
 * @brief Mark local indexes (for partitions) or normal indexes (for ordinary tables) as unusable.
 *
 * This function handles index invalidation for both partitioned tables (specifically their partitions)
 * and ordinary tables.
 * - For partitions: It iterates over the partition's index list (from rd_indexlist) and marks
 *   each index partition as unusable. Note that index partitions are not full Relations, so
 *   index_open is not used.
 * - For ordinary tables: It retrieves the index list, opens each index relation, marks it as
 *   unusable, and then closes it.
 *
 * @param[in] rel The relation (either a table partition or an ordinary table) whose indexes need to be invalidated.
 */
static void rel_shrink_normal_idx_unusable(Relation rel)
{
    List* indexList = NIL;
    ListCell* lc = NULL;
    if (RelationIsPartition(rel)) {
        indexList = rel->rd_indexlist;
        foreach (lc, indexList) {
            Oid indexOid = lfirst_oid(lc);
            /*
             * For partitions, index objects are not Relations (they are in pg_partition),
             * so we cannot use index_open. The AccessExclusiveLock on the data partition
             * (held by caller) provides sufficient protection.
             */
            ATExecSetIndexUsableState(PartitionRelationId, indexOid, false);
            CacheInvalidateRelcacheByRelid(indexOid);
        }
    } else {
        indexList = RelationGetIndexList(rel);
        foreach (lc, indexList) {
            Oid indexOid = lfirst_oid(lc);
            Relation indexRel = index_open(indexOid, RowExclusiveLock);
            ATExecSetIndexUsableState(IndexRelationId, indexOid, false);
            index_close(indexRel, RowExclusiveLock);
            CacheInvalidateRelcacheByRelid(indexOid);
        }
        list_free(indexList);
    }

    ereport(WARNING,
            (errmodule(MOD_SEGMENT_PAGE),
             errmsg("[SEG_SHRINK] relation \"%s\" <%u/%u/%u> shrink may cause some indexes to become logically "
                    "inconsistent, so the affected indexes have become unusable and must be rebuilt.",
                    RelationGetRelationName(rel), rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode)));
}

/**
 * @brief Shrinks the space of a segment-page table by releasing unused extents.
 *
 * This function is the core logic for segment-page shrink. It ensures the target
 * relation is a normal table in current database and tablespace. Then it calls
 * seg_shrink_relation_space to perform extent shrinking, flushes relation buffers,
 * and invalidates relcache.
 *
 * @param[in] rel The Relation to shrink
 * @param[in] spaceid OID of the tablespace (must match relation)
 * @param[in] dbid OID of the database (must match relation)
 * @param[out] replace_extents Output pointer to store the count of moved (replaced) extents.
 * @return Number of extents successfully reclaimed
 */
static uint32 rel_shrink_extents(Relation rel, Oid spaceid, Oid dbid, uint32* replace_extents)
{
    if (!RelationIsSegmentTable(rel)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("gs_table_shrink only support segment-page table")));
    }

    if (rel->rd_rel->relkind != RELKIND_RELATION) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("gs_table_shrink only support ordinary table")));
    }

    if (rel->rd_node.spcNode != spaceid || rel->rd_node.dbNode != dbid) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("gs_table_shrink only support current db and space")));
    }

    if (IS_SEG_COMPRESSED_RNODE(rel->rd_node, MAIN_FORKNUM)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("gs_table_shrink do not support compress table")));
    }

    uint32 replace_cnt = 0;
    uint32 shrink_extents = seg_shrink_relation_space(rel, &replace_cnt);
    if (replace_cnt > 0) {
        rel_shrink_normal_idx_unusable(rel);
        if (replace_extents != NULL) {
            *replace_extents = replace_cnt;
        }
    }

    FlushRelationBuffers(rel);
    DropRelFileNodeShareBuffers(rel->rd_node, MAIN_FORKNUM, 0);
    CacheInvalidateRelcache(rel);
    return shrink_extents;
}

static uint32 rel_shrink_partition_table(Relation rel, Oid spaceId, Oid dbid, bool* execFailed)
{
    uint32 shrinkExtents = 0;
    List* partOidList = relationGetPartitionOidList(rel);
    ListCell* cell = NULL;
    Partition part = NULL;
    Relation partRel = NULL;
    uint32 total_replace_cnt = 0;
    *execFailed = false;

    foreach (cell, partOidList) {
        if (*execFailed) {
            break;
        }

        Oid partOid = lfirst_oid(cell);
        PG_TRY();
        {
            part = partitionOpen(rel, partOid, AccessExclusiveLock);
            partRel = partitionGetRelation(rel, part);
            uint32 replace_cnt = 0;
            shrinkExtents += rel_shrink_extents(partRel, spaceId, dbid, &replace_cnt);
            releaseDummyRelation(&partRel);
            partitionClose(rel, part, NoLock);
            total_replace_cnt += replace_cnt;
            part = NULL;
            partRel = NULL;
        }
        PG_CATCH();
        {
            if (partRel != NULL) {
                releaseDummyRelation(&partRel);
                partRel = NULL;
            }
            if (part != NULL) {
                partitionClose(rel, part, NoLock);
                part = NULL;
            }

            EmitErrorReport();
            FlushErrorState();
            *execFailed = true;
        }
        PG_END_TRY();
    }

    if (partOidList != NULL) {
        releasePartitionOidList(&partOidList);
    }

    if (total_replace_cnt > 0) {
        rel_shrink_global_idx_unusable(rel);
    }

    CacheInvalidateRelcache(rel);
    return shrinkExtents;
}

/**
 * @brief SQL-callable interface to shrink a specific segment-page table by reclaiming unused tail extents.
 *
 * This function performs table-level space shrinking by identifying and reclaiming unused extents
 * at the end of the table's segment allocation. It supports both regular tables and partitioned
 * tables (excluding sub-partitioned tables). The operation requires exclusive access to prevent
 * concurrent modifications during the shrinking process.
 *
 * Supported table types:
 * - Segment-page tables (segment=on)
 * - Ordinary relations (RELKIND_RELATION)
 * - Partitioned tables (non-sub-partitioned)
 *
 * Restrictions:
 * - Only works on current database and tablespace
 * - Does not support compressed tables
 * - Requires recovery not in progress
 * - Not available on standby nodes
 *
 * @param[in] tableName The name of the relation to be shrunk
 * @param[in] tablespaceName Name of the tablespace (must match table's tablespace)
 * @param[in] dbName Name of the database (must be current database)
 * @return Number of total extents reclaimed (sum across all partitions if applicable)
 *
 * @note This operation flushes relation buffers and invalidates relcache after shrinking
 * @warning Requires AccessExclusiveLock on the target relation
 */
Datum gs_table_shrink(PG_FUNCTION_ARGS)
{
    if (!XLogInsertAllowed()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot shrink space, for recovery is in progress.")));
    }

    if (SS_STANDBY_MODE) {
        ereport(ERROR, (errmsg("SS standby cannot execute gs_table_shrink")));
    }

    if (SS_DISASTER_STANDBY_CLUSTER) {
        ereport(ERROR, (errmsg("SS disaster standby cluster cannot execute gs_table_shrink")));
    }

    char* relName = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* tableSpaceName = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* dbName = text_to_cstring(PG_GETARG_TEXT_PP(2));

    Oid dbid = get_database_oid_by_name(dbName);
    if (dbid != u_sess->proc_cxt.MyDatabaseId) {
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errmsg("database id is not current database")));
    }
    Oid relOid = RelnameGetRelid(relName);
    if (!OidIsValid(relOid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s\" does not exist", relName)));
    }
    Oid spaceId = get_tablespace_oid_by_name(tableSpaceName);
    if (!OidIsValid(spaceId)) {
        ereport(ERROR, (errmsg("invalid tablespace \"%s\"", tableSpaceName)));
    }

    Relation rel = relation_open(relOid, AccessExclusiveLock);
    AclResult aclresult = pg_class_aclcheck(relOid, GetUserId(), ACL_VACUUM);
    if (aclresult != ACLCHECK_OK && !(pg_class_ownercheck(relOid, GetUserId()) ||
            (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) && !rel->rd_rel->relisshared))) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied for relation %s, required VACUUM permissions to shrink", relName)));
    }

    uint32 shrinkExtents = 0;
    bool execFailed = false;
    if (!RelationIsValid(rel)) {
        relation_close(rel, NoLock);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not open relation \"%s\"", relName)));
    }

    if (!RelationIsRelation(rel)) {
        relation_close(rel, NoLock);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("can not shrink relation \"%s\", as it is not a ordinary table", relName)));
    }

    if (RELATION_IS_PARTITIONED(rel)) {
        if (RelationIsSubPartitioned(rel)) {
            relation_close(rel, NoLock);
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("can not shrink relation \"%s\", function gs_table_shrink don't support subpartition table",
                            relName)));
        }
        shrinkExtents = rel_shrink_partition_table(rel, spaceId, dbid, &execFailed);
    } else {
        PG_TRY();
        {
            shrinkExtents = rel_shrink_extents(rel, spaceId, dbid, NULL);
        }
        PG_CATCH();
        {
            EmitErrorReport();
            FlushErrorState();
            execFailed = true;
        }
        PG_END_TRY();
    }

    relation_close(rel, NoLock);
    if (execFailed) {
        ereport(ERROR, (errmsg("gs_table_shrink execute failed")));
    }

    pfree(relName);
    pfree(tableSpaceName);
    pfree(dbName);
    return shrinkExtents;
}

/**
 * @brief Compact segment space by moving tail extents to earlier free positions.
 *
 * This function implements the core compaction algorithm that eliminates fragmentation
 * by relocating valid data extents from higher block numbers to available free slots
 * at lower positions. The process continues until no more beneficial moves can be made
 * (i.e., when the next victim extent is at a lower position than available free space).
 *
 * Algorithm:
 * 1. Initialize victim selector with compact_search=true (searches from tail backwards)
 * 2. For each victim extent found:
 *    - Find the earliest available free extent
 *    - If free extent position >= victim position, stop (no benefit)
 *    - Otherwise, move the victim extent to the free position
 * 3. Continue until no more beneficial moves exist
 *
 * The compaction uses the segment's BMT (Block Mapping Tree) to update logical-to-physical
 * mappings transparently, ensuring that all existing references (indexes, HOT chains, etc.)
 * remain valid through the logical page number abstraction.
 *
 * @param[in] seg Pointer to SegExtentGroup representing the segment group to compact
 * @param[in] forknum Fork number (typically MAIN_FORKNUM for table data)
 *
 * @note This operation requires careful coordination with the buffer manager to handle
 *       pages that may be in memory during the move process
 * @see move_one_extent() for individual extent movement logic
 * @see ShrinkVictimSelector for extent selection strategy
 */
static void move_extents_compact(SegExtentGroup* seg, ForkNumber forknum)
{
    /*
     * Copy meta-data from map head, and release the buffer.
     * Each time, we (1) select one extent (2) move it. Step (1) and (2) require locks independently to avoid deadlock.
     */
    ShrinkVictimSelector selector;
    selector.init(seg, InvalidBlockNumber, true);

    Buffer ipbuf = InvalidBuffer;
    BlockNumber victim = selector.next();
    while (victim != InvalidBlockNumber) {
        CHECK_FOR_INTERRUPTS();
        BlockNumber free_ext = eg_search_free_extent(seg);
        if (free_ext >= victim) {
            break;
        }
        move_one_extent(seg, victim, &ipbuf, forknum);
        victim = selector.next();
    }

    if (BufferIsValid(ipbuf)) {
        SegReleaseBuffer(ipbuf);
    }
}

/**
 * @brief Update the high water mark of a segment if there are trailing free extents.
 *
 * This function scans the extent bitmap pages from high to low and identifies the
 * new high water mark by skipping trailing free extents. If a lower HWM is found,
 * it updates metadata and writes WAL record.
 *
 * @param[in] seg Pointer to SegExtentGroup representing the segment group
 * @return New high water mark if updated, else old HWM
 */
static BlockNumber update_shrink_hwm(SegExtentGroup* seg)
{
    Buffer buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, seg->map_head_entry, RBM_NORMAL);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    df_map_head_t* map_head = (df_map_head_t*)PageGetContents(BufferGetBlock(buffer));
    BlockNumber hwm = map_head->high_water_mark;
    BlockNumber new_hwm = hwm;
    bool end = false;
    int i = map_head->group_count - 1;
    uint16 new_count = map_head->group_count;
    for (; !end && i >= 0; i--) {
        new_count = i + 1;
        BlockNumber first_map_block = map_head->groups[i].first_map;
        for (int j = map_head->groups[i].page_count - 1; !end && j >= 0; j--) {
            BlockNumber map_block = first_map_block + j;

            Buffer map_buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, map_block, RBM_NORMAL);
            LockBuffer(map_buffer, BUFFER_LOCK_SHARE);
            df_map_page_t* map_page = (df_map_page_t*)PageGetContents(BufferGetBlock(map_buffer));

            if (map_page->first_page >= hwm) {
                // extents managed by this map block must be un-allocated, because they exceed the range of hwm.
                SegUnlockReleaseBuffer(map_buffer);
                continue;
            }

            int last_bit = map_page->dirty_last;
            for (; last_bit >= 0; last_bit--) {
                BlockNumber extent = map_page->first_page + seg->extent_size * last_bit;
                if (extent >= hwm) {
                    SegmentCheck(DF_MAP_FREE(map_page->bitmap, last_bit));
                    continue;
                }

                if (DF_MAP_FREE(map_page->bitmap, last_bit)) {
                    new_hwm = extent;
                } else {
                    // the extent is used, we can not shrink the hwm anymore.
                    end = true;
                    break;
                }
            }

            SegUnlockReleaseBuffer(map_buffer);
        }
    }

    if (new_hwm < hwm) {
        SegmentCheck(new_count <= map_head->group_count);
        ereport(
            LOG,
            (errmodule(MOD_SEGMENT_PAGE),
             errmsg(
                 "[SEG_SHRINK] shrink triggers SegExtentGroup <%u/%u/%u> refresh hwm, high water mark from %u to %u, "
                 "group count from %u to %u",
                 seg->rnode.spcNode, seg->rnode.dbNode, seg->rnode.relNode, hwm, new_hwm, map_head->group_count,
                 new_count)));

        START_CRIT_SECTION();
        XLogAtomicOpStart();
        XLogDataUpdateSpaceHWM xlog_data;
        xlog_data.new_hwm = new_hwm;
        xlog_data.old_hwm = hwm;
        xlog_data.old_groupcnt = map_head->group_count;
        xlog_data.new_groupcnt = new_count;
        XLogAtomicOpRegisterBuffer(buffer, REGBUF_KEEP_DATA, SPCXLOG_SPACE_UPDATE_HWM, XLOG_COMMIT_KEEP_BUFFER_STATE);
        XLogAtomicOpRegisterBufData((char*)&xlog_data, sizeof(XLogDataUpdateSpaceHWM));
        XLogAtomicOpCommit();
        END_CRIT_SECTION();

        map_head->high_water_mark = new_hwm;
        map_head->group_count = new_count;
        SegUnlockReleaseBuffer(buffer);
        return new_hwm;
    }

    SegUnlockReleaseBuffer(buffer);
    return hwm;
}

/**
 * @brief Calculate the physical high water mark based on data and meta usage.
 *
 * Determines the maximum block number for data/meta blocks to avoid unnecessary
 * space reservation during shrinking.
 *
 * @param[in] seg Pointer to SegExtentGroup representing the segment group
 * @param[in] new_hwm high water mark (block number)
 * @return BlockNumber Physical block boundary for the extent group.
 */
static BlockNumber calc_physical_hwm(SegExtentGroup* seg, BlockNumber new_hwm)
{
    Buffer buffer = ReadBufferFast(seg->space, seg->rnode, seg->forknum, seg->map_head_entry, RBM_NORMAL);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    df_map_head_t* map_head = (df_map_head_t*)PageGetContents(BufferGetBlock(buffer));
    BlockNumber extents = map_head->allocated_extents;
    SegUnlockReleaseBuffer(buffer);

    BlockNumber data_blocks = extents * seg->extent_size;
    data_blocks += DF_FILE_EXTEND_STEP_BLOCKS;  // contains extra 128 MB data

    /* First map group */
    BlockNumber meta_blocks = DEFAULT_META_BLOCKS;
    BlockNumber group_blocks = seg->extent_size * DF_MAP_GROUP_SIZE * DF_MAP_BIT_CNT;
    while (data_blocks > group_blocks) {
        /* If current map group can not contain the rest data blocks, we need more map group */
        data_blocks -= group_blocks;
        meta_blocks += DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE;
    }
    BlockNumber target_size = data_blocks + meta_blocks;
    BlockNumber final_target = (new_hwm > target_size) ? new_hwm : target_size;
    final_target = CM_ALIGN_ANY(final_target, DF_FILE_EXTEND_STEP_BLOCKS);
    BlockNumber df_size = eg_df_size(seg);
    SegmentCheck((df_size % DF_FILE_EXTEND_STEP_BLOCKS) == 0);
    return (final_target < df_size) ? final_target : InvalidBlockNumber;
}

/**
 * @brief Perform shrink and compaction on all segment extent groups of a given type.
 *
 * It first moves extents to compact the allocation. Then it recalculates the HWM
 * and if physical file size can be reduced, it triggers file shrinking.
 *
 * @param[in] spcNode Tablespace OID
 * @param[in] dbNode Database OID
 * @param[in] extentType Extent size type (e.g., EXTENT_8 to EXTENT_8192)
 * @param[in] forknum Fork number (e.g., MAIN_FORKNUM)
 */
static void spc_shrink_compact(Oid spcNode, Oid dbNode, int extentType, ForkNumber forknum)
{
    SegmentCheck(extentType >= EXTENT_8 && extentType <= EXTENT_8192);

    SegSpace *spc = spc_open(spcNode, dbNode, false);
    if (spc == NULL) {
        ereport(ERROR, (errmsg("Segment is not initialized in current database, database and "
                               "tablespace may be incorrect or mismatched")));
    }
    SegExtentGroup* seg = &spc->extent_group[EXTENT_TYPE_TO_GROUPID(extentType)][forknum];
    if (!eg_df_exists(seg)) {
        ereport(LOG, (errmsg("Segment is not initialized in current database")));
        return;
    }

    move_extents_compact(seg, forknum);

    /*
     * We must lock the segment extent group here, to forbid any extent allocation.
     */
    AutoMutexLock spc_lock(&spc->lock);
    spc_lock.lock();

    BlockNumber new_hwm = update_shrink_hwm(seg);
    BlockNumber physical_hwm = calc_physical_hwm(seg, new_hwm);
    if (BlockNumberIsValid(physical_hwm)) {
        spc_shrink_files(seg, physical_hwm, false);
    }
}

/**
 * @brief Perform comprehensive space shrinking and compaction across all extent groups in a tablespace.
 *
 * This function executes a two-phase space optimization process:
 * 1. **Compaction Phase**: Moves valid data extents from tail positions to earlier free slots
 *    to eliminate fragmentation and create contiguous free space at the end
 * 2. **Shrinking Phase**: Recalculates high water mark (HWM) and physically truncates
 *    segment files to release unused space back to the filesystem
 *
 * The operation processes all extent types (EXTENT_8, EXTENT_64, EXTENT_1024, EXTENT_8192)
 * within the specified tablespace and database, focusing on MAIN_FORKNUM.
 * Process flow:
 *   - For each extent type: call move_extents_compact() to relocate tail extents
 *   - Lock extent group to prevent concurrent allocations
 *   - Update logical and physical high water marks
 *   - Truncate physical files if space can be reclaimed
 *
 * @param[in] tablespaceName Name of the tablespace to compact
 * @param[in] dbName Name of the database (must be current database)
 * @return Datum Always returns 0 on success.
 */
Datum gs_space_shrink_compact(PG_FUNCTION_ARGS)
{
    if (!XLogInsertAllowed()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot shrink space, for recovery is in progress.")));
    }

    if (SS_STANDBY_MODE) {
        ereport(ERROR, (errmsg("SS standby cannot execute gs_space_shrink_compact")));
    }

    if (SS_DISASTER_STANDBY_CLUSTER) {
        ereport(ERROR, (errmsg("SS disaster standby cluster cannot execute gs_space_shrink_compact")));
    }

    char* tableSpaceName = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* dbName = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid spaceid = get_tablespace_oid_by_name(tableSpaceName);
    if (!OidIsValid(spaceid)) {
        ereport(ERROR, (errmsg("invalid tablespace \"%s\"", tableSpaceName)));
    }

    Oid dbid = get_database_oid_by_name(dbName);
    if (dbid != u_sess->proc_cxt.MyDatabaseId) {
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errmsg("database id is not current database")));
    }

    AclResult aclresult = pg_tablespace_aclcheck(spaceid, GetUserId(), ACL_VACUUM);
    if (aclresult != ACLCHECK_OK) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied for tablespace %s, required VACUUM permissions to shrink", tableSpaceName)));
    }

    for (int extent_type = EXTENT_8; extent_type <= EXTENT_8192; extent_type++) {
        spc_shrink_compact(spaceid, dbid, extent_type, MAIN_FORKNUM);
    }

    pfree(tableSpaceName);
    pfree(dbName);
    return 0;
}
