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
#include "commands/tablespace.h"
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
    int egid = EXTENT_TYPE_TO_GROUPID(relNode.relNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];

    df_pread_block(seg->segfile, buffer, blocknum);
}

void spc_write_block(SegSpace *spc, RelFileNode relNode, ForkNumber forknum, const char *buffer, BlockNumber blocknum)
{
    int egid = EXTENT_TYPE_TO_GROUPID(relNode.relNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];

    df_pwrite_block(seg->segfile, buffer, blocknum);
}

void spc_writeback(SegSpace *spc, int extent_size, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks)
{
    int egid = EXTENT_SIZE_TO_GROUPID(extent_size);
    SegLogicFile *sf = spc->extent_group[egid][forknum].segfile;

    df_flush_data(sf, blocknum, nblocks);
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

/*
 * Check whether the space is empty, if so, drop all metadata buffers.
 *
 * Space object in hash table is not removed. Just set as closed.
 */
SegSpace *spc_drop(Oid spcNode, Oid dbNode, bool redo)
{
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

    BlockNumber data_blocks = extents * seg->extent_size;
    data_blocks += DF_FILE_EXTEND_STEP_BLOCKS; // contains extra 128 MB data

    /* First map group */
    BlockNumber meta_blocks = DF_MAP_GROUP_SIZE + DF_MAP_HEAD_PAGE + 1 + IPBLOCK_GROUP_SIZE;
    *new_group_count = 1;
    BlockNumber group_blocks = seg->extent_size * DF_MAP_GROUP_SIZE * DF_MAP_BIT_CNT;
    while (data_blocks > group_blocks) {
        /* If current map group can not contain the rest data blocks, we need more map group */
        data_blocks -= group_blocks;
        (*new_group_count)++;
        meta_blocks += DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE;
    }

    *target_size = data_blocks + meta_blocks;
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
    LWLock *partition_lock = BufMappingPartitionLock(hashcode);

    LWLockAcquire(partition_lock, LW_SHARED);
    int buf_id = BufTableLookup(&tag, hashcode);
    if (buf_id >= 0) {
        BufferDesc *buf = GetBufferDescriptor(buf_id);

        ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

        /* Pin the buffer to avoid invalidated by others */
        bool valid = PinBuffer(buf, NULL);

        LWLockRelease(partition_lock);

        if (!valid) {
            UnpinBuffer(buf, true);
            return InvalidBuffer;
        }

        return BufferDescriptorGetBuffer(buf);
    }
    LWLockRelease(partition_lock);

    return InvalidBuffer;
}

/*
 * logic_rnode and logic_start_blocknum is used to get blocks' buffer if possible
 */
static void copy_extent(SegExtentGroup *seg, RelFileNode logic_rnode, uint32 logic_start_blocknum,
                        BlockNumber nblocks, BlockNumber phy_from_extent, BlockNumber phy_to_extent)
{
    char *content = (char *)palloc(BLCKSZ);
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
        /*
         * In the extent to be moved, there may exist some blocks cached in shared buffer. Thus we need to
         * copy content from buffer instead of file. Note that we can not use 'ReadBuffer' interface directly,
         * because if buffer does not exist, it will try to allocate a new buffer and invoking smgrread that
         * requires segment head lock. However, segment head lock has been held already here. Invoking ReadBuffer
         * leads to dead lock.
         *
         */
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
            uint32 buf_state = LockBufHdr(buf_desc);
            UnlockBufHdr(buf_desc, buf_state);
            if (buf_state & BM_DIRTY) {
                FlushOneBufferIncludeDW(buf_desc);
            }
        } else {
            BlockNumber from_block = phy_from_extent + i;
            df_pread_block(seg->segfile, content, from_block);
            pagedata = content;
        }

        BlockNumber to_block = phy_to_extent + i;

        START_CRIT_SECTION();
        {
            BufferTag tag = {
                .rnode = seg->rnode,
                .forkNum = seg->forknum,
                .blockNum = to_block
            };
            /* 1. xlog insert and set lsn */
            XLogBeginInsert();
            XLogRegisterData((char *)&tag, sizeof(tag));
            XLogRegisterData(pagedata, BLCKSZ);
            XLogRecPtr recptr = XLogInsert(RM_SEGPAGE_ID, XLOG_SEG_NEW_PAGE);
            PageSetLSN(pagedata, recptr);

            /* 2. double write */
            bool flush_old_file = false;
            uint32 pos = seg_dw_single_flush_without_buffer(tag, (Block)pagedata, &flush_old_file);
            t_thrd.proc->dw_pos = pos;
            t_thrd.proc->flush_new_dw = !flush_old_file;

            /* 3. checksum and write to file */
            PageSetChecksumInplace((Page)pagedata, to_block);
            df_pwrite_block(seg->segfile, pagedata, to_block);
            if (flush_old_file) {
                g_instance.dw_single_cxt.recovery_buf.single_flush_state[pos] = true;
            } else {
                g_instance.dw_single_cxt.single_flush_state[pos] = true;
            }
            t_thrd.proc->dw_pos = -1;
        }
        END_CRIT_SECTION();

        SEGMENTTEST(SEGMENT_COPY_BLOCK, (errmsg("error happens just after copy one block")));

        if (BufferIsValid(buf)) {
            BufferDesc *bufdesc = GetBufferDescriptor(buf - 1);
            SegmentCheck(bufdesc->seg_fileno == seg->rnode.relNode);
            bufdesc->seg_blockno = to_block;

            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            UnpinBuffer(bufdesc, true);
        }
    }
    pfree(content);
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
    Oid relation_oid = RelidByRelfilenode(spcNode, relNode, true);
    if (!OidIsValid(relation_oid)) {
        /* Try pg_partition */
        Oid toastid, partition_oid;
        relation_oid = PartitionRelidByRelfilenode(spcNode, relNode, toastid, &partition_oid, true);
    }
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

void move_data_extent(SegExtentGroup *seg, BlockNumber extent, ExtentInversePointer iptr)
{
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
    BlockNumber logic_start = ExtentIdToLogicBlockNum(extent_id);

    XLogAtomicOpStart();

    /* Inverse pointer for the new extent is the same as the old extent */
    BlockNumber new_extent = eg_alloc_extent(seg, InvalidBlockNumber, iptr);

    /* eg_alloc_extent implicate START_CRIT_SECTION; We do not worry any Error during copy_extent */
    copy_extent(seg, logic_rnode, logic_start, owner_seghead->nblocks, extent, new_extent);

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
}

void move_one_extent(SegExtentGroup *seg, BlockNumber extent, Buffer *ipbuf)
{
    ExtentInversePointer iptr = GetInversePointer(seg, extent, ipbuf);
    ExtentUsageType usage = SPC_INVRSPTR_GET_USAGE(iptr);

    if (usage == DATA_EXTENT) {
        move_data_extent(seg, extent, iptr);
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

    void init(SegExtentGroup *seg, BlockNumber target_size);
    BlockNumber next();
};

void ShrinkVictimSelector::init(SegExtentGroup *seg, BlockNumber target_size)
{
    this->target_size = target_size;

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
                if (extent + seg->extent_size <= this->target_size) {
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
void move_extents(SegExtentGroup *seg, BlockNumber target_size)
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
        move_one_extent(seg, victim, &ipbuf);
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
        uint32 state;

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
    move_extents(seg, target_size);

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

    Oid spaceid = PG_GETARG_OID(0);
    Oid dbid = PG_GETARG_OID(1);
    uint32 extent_type = PG_GETARG_UINT32(2);
    ForkNumber forknum = PG_GETARG_INT32(3);

    return gs_space_shrink_internal(spaceid, dbid, extent_type, forknum);
}

Datum local_space_shrink(PG_FUNCTION_ARGS)
{
    if (!XLogInsertAllowed()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Don't shrink space locally, for recovery is in progress.")));
    }

    char *tablespacename = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *dbname = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid spaceid = get_tablespace_oid_by_name(tablespacename);
    Oid dbid = get_database_oid_by_name(dbname);

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
    appendStringInfo(&buf, "select local_space_shrink(\'%s\', \'%s\')", tablespacename, dbname);
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

