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
 * data_file.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segment/data_file.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/storage_xlog.h"
#include "commands/tablespace.h"
#include "commands/verify.h"
#include "executor/executor.h"
#include "pgstat.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/knl_usync.h"
#include "storage/smgr/segment.h"
#include "postmaster/pagerepair.h"

static const mode_t SEGMENT_FILE_MODE = S_IWUSR | S_IRUSR;

static int dv_open_file(char *filename, uint32 flags, int mode);
static void dv_close_file(int fd);
static void df_open_target_files(SegLogicFile *sf, int targetno);

static char *slice_filename(const char *filename, int sliceno);
void df_extend_internal(SegLogicFile *sf);

/*
 * We can not use virtual fd because space data files are accessed by multi-thread.
 * Callers must handle fd < 0
 */
static int dv_open_file(char *filename, uint32 flags, int mode)
{
    int fd = -1;
    fd = BasicOpenFile(filename, flags, mode);
    int err = errno;
    ereport(LOG, (errmsg("dv_open_file filename: %s, flags is %u, mode is %d, fd is %d", filename, flags, mode, fd)));
    errno = err;
    return fd;
}

static void dv_close_file(int fd)
{
    close(fd);
    ereport(LOG, (errmsg("dv_close_file fd is %d", fd)));
}

/* Return a palloc string, and callers should free it */
static char *slice_filename(const char *filename, int sliceno)
{
    char *res = (char *)palloc(MAXPGPATH);
    if (sliceno == 0) {
        errno_t rc = snprintf_s(res, MAXPGPATH, MAXPGPATH - 1, "%s", filename);
        securec_check_ss(rc, "\0", "\0");
    } else {
        errno_t rc = snprintf_s(res, MAXPGPATH, MAXPGPATH - 1, "%s.%d", filename, sliceno);
        securec_check_ss(rc, "\0", "\0");
    }
    return res;
}

/*
 * Create the first physical file for the logic file.
 *
 * SegLogicFile must be opened before invoking this function.
 */
void df_create_file(SegLogicFile *sf, bool redo)
{
    char *filename = slice_filename(sf->filename, 0);

    if (sf->file_num > 0) {
        // File exists, ensure its size.
        SegmentCheck(redo);
        if (sf->total_blocks < DF_FILE_EXTEND_STEP_BLOCKS) {
            SegmentCheck(sf->file_num == 1);
            int fd = sf->segfiles[0].fd;
            if (ftruncate(fd, DF_FILE_EXTEND_STEP_SIZE) != 0) {
                ereport(PANIC,
                        (errmsg("ftuncate file %s failed during df_extend due to %s", filename, strerror(errno))));
            }
        }
    } else {
        // File not exists
        uint32 flags = O_RDWR | O_CREAT | O_EXCL | PG_BINARY;
        if (sf->segfiles != NULL) {
            ereport(LOG,
                (errmodule(MOD_SEGMENT_PAGE),
                    errmsg("[segpage] sf->segfiles is not null, last invocation of df_create_file must be failed "
                           "halfway. spc/db/relnode/fork: %u/%u/%d/%d",
                        sf->relNode.spcNode,
                        sf->relNode.dbNode,
                        sf->relNode.relNode,
                        sf->forknum)));
        } else {
            MemoryContext oldcnxt = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
            sf->segfiles = (SegPhysicalFile*)palloc(sizeof(SegPhysicalFile) * DF_ARRAY_EXTEND_STEP);
            MemoryContextSwitchTo(oldcnxt);

            for (int i = 0; i < DF_ARRAY_EXTEND_STEP; i++) {
                sf->segfiles[i].fd = -1;
            }
            sf->vector_capacity = DF_ARRAY_EXTEND_STEP;
        }

        SegPhysicalFile first_file;
        first_file.sliceno = 0;
        first_file.fd = dv_open_file(filename, flags, SEGMENT_FILE_MODE);
        if (first_file.fd < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("[segpage] could not create file \"%s\": %m", filename)));
        } else {
            ereport(LOG, (errcode_for_file_access(), errmsg("[segpage] Success create file \"%s\"", filename)));
        }

        sf->file_num = 1;
        sf->segfiles[0] = first_file;

        /* ensure place for MapHeadPage */
        df_extend_internal(sf);
    }
    pfree(filename);
}

static SegPhysicalFile df_get_physical_file(SegLogicFile *sf, int sliceno, BlockNumber target_block)
{
    AutoMutexLock filelock(&sf->filelock);
    filelock.lock();
    if (target_block == InvalidBlockNumber) {
        SegmentCheck(0);
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("df_get_physical_file target_block is InvalidBlockNumber!\n")));
    }

    if (sf->total_blocks <= target_block) {
        ereport(LOG,
                (errmodule(MOD_SEGMENT_PAGE), errmsg("Try to access file %s block %u, exceeds the file total blocks %u",
                                                     sf->filename, target_block, sf->total_blocks)));
        SegmentCheck(RecoveryInProgress());
        while (sf->total_blocks <= target_block) {
            df_extend_internal(sf);
        }
    }

    SegmentCheck(sliceno < sf->file_num);

    SegPhysicalFile spf = sf->segfiles[sliceno];
    return spf;
}

void df_flush_data(SegLogicFile *sf, BlockNumber blocknum, BlockNumber nblocks)
{
    int128 remainBlocks = nblocks;
    while (remainBlocks > 0) {
        BlockNumber nflush = remainBlocks;
        int slice_start = blocknum / DF_FILE_SLICE_BLOCKS;
        int slice_end = (blocknum + remainBlocks - 1) / DF_FILE_SLICE_BLOCKS;

        /* cross slice */
        if (slice_start != slice_end) {
            nflush = DF_FILE_SLICE_BLOCKS - (blocknum % DF_FILE_SLICE_BLOCKS);
        }

        SegPhysicalFile spf = df_get_physical_file(sf, slice_start, blocknum);
        if (spf.fd < 0) {
            return;
        }

        off_t seekpos = (off_t)BLCKSZ * (blocknum % DF_FILE_SLICE_BLOCKS);
        pg_flush_data(spf.fd, seekpos, (off_t)nflush * BLCKSZ);

        remainBlocks -= nflush;
        blocknum += nflush;
    }
}

/*
 * Extend the file array in SegLogicFile
 */
void df_extend_file_vector(SegLogicFile *sf)
{
    int new_capacity = sf->vector_capacity + DF_ARRAY_EXTEND_STEP;
    MemoryContext oldcnxt = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    SegPhysicalFile *newfiles = (SegPhysicalFile *)palloc(sizeof(SegPhysicalFile) * new_capacity);
    MemoryContextSwitchTo(oldcnxt);

    for (int i = 0; i < sf->file_num; i++) {
        newfiles[i] = sf->segfiles[i];
    }

    if (sf->segfiles != NULL) {
        pfree(sf->segfiles);
    }
    sf->segfiles = newfiles;
    sf->vector_capacity = new_capacity;
}

void df_close_all_file(RepairFileKey key, int32 max_sliceno)
{
    Oid relNode = key.relfilenode.relNode;
    ForkNumber forknum = key.forknum;
    struct stat statBuf;
    SegSpace *spc = spc_init_space_node(key.relfilenode.spcNode, key.relfilenode.dbNode);

    Assert(relNode <= EXTENT_8192 && relNode > 0);
    AutoMutexLock spc_lock(&spc->lock);
    spc_lock.lock();

    SegExtentGroup *eg = &spc->extent_group[relNode - 1][forknum];
    AutoMutexLock lock(&eg->lock);
    lock.lock();

    AutoMutexLock filelock(&eg->segfile->filelock);
    filelock.lock();

    char *tempfilename = NULL;
    for (int i = 0; i <= max_sliceno; i++) {
        tempfilename = slice_filename(eg->segfile->filename, i);
        if (stat(tempfilename, &statBuf) < 0) {
            /* ENOENT is expected after the last segment... */
            if (errno != ENOENT) {
                ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not stat file \"%s\": %m", tempfilename)));
            }
        } else {
            if (eg->segfile->segfiles[i].fd > 0) {
                dv_close_file(eg->segfile->segfiles[i].fd);
                eg->segfile->segfiles[i].fd = -1;
            }
        }
        pfree(tempfilename);
        eg->segfile->segfiles[i].fd = -1;
        eg->segfile->segfiles[i].sliceno = i;
    }
    eg->segfile->file_num = 0;

    filelock.unLock();
    lock.unLock();
    spc_lock.unLock();
    return;
}

void df_clear_and_close_all_file(RepairFileKey key, int32 max_sliceno)
{
    Oid relNode = key.relfilenode.relNode;
    ForkNumber forknum = key.forknum;
    SegSpace *spc = spc_init_space_node(key.relfilenode.spcNode, key.relfilenode.dbNode);

    Assert(relNode <= EXTENT_8192 && relNode > 0);
    AutoMutexLock spc_lock(&spc->lock);
    spc_lock.lock();

    SegExtentGroup *eg = &spc->extent_group[relNode - 1][forknum];
    AutoMutexLock lock(&eg->lock);
    lock.lock();

    AutoMutexLock filelock(&eg->segfile->filelock);
    filelock.lock();

    char *tempfilename = NULL;
    for (int i = 0; i <= max_sliceno; i++) {
        tempfilename = slice_filename(eg->segfile->filename, i);
        int ret = unlink(tempfilename);
        if (ret < 0 && errno != ENOENT) {
            ereport(WARNING, (errcode_for_file_access(),
                errmsg("could not remove file \"%s\": %m", tempfilename)));
        }
        if (ret >= 0) {
            ereport(LOG, (errcode_for_file_access(),
                errmsg("[file repair] clear segment file \"%s\"", tempfilename)));
            if (eg->segfile->segfiles[i].fd > 0) {
                dv_close_file(eg->segfile->segfiles[i].fd);
                eg->segfile->segfiles[i].fd = -1;
            }
        }
        pfree(tempfilename);
        eg->segfile->segfiles[i].fd = -1;
        eg->segfile->segfiles[i].sliceno = i;
    }
    eg->segfile->file_num = 0;

    filelock.unLock();
    lock.unLock();
    spc_lock.unLock();
    return;
}

void df_open_all_file(RepairFileKey key, int32 max_sliceno)
{
    int fd = -1;
    char *filename = NULL;
    uint32 flags = O_RDWR | PG_BINARY;
    Oid relNode = key.relfilenode.relNode;
    ForkNumber forknum = key.forknum;
    SegSpace *spc = spc_init_space_node(key.relfilenode.spcNode, key.relfilenode.dbNode);

    Assert(relNode <= EXTENT_8192 && relNode > 0);
    AutoMutexLock spc_lock(&spc->lock);
    spc_lock.lock();

    SegExtentGroup *eg = &spc->extent_group[relNode - 1][forknum];
    AutoMutexLock eg_lock(&eg->lock);
    eg_lock.lock();

    AutoMutexLock file_lock(&eg->segfile->filelock);
    file_lock.lock();

    for (int i = 0; i <= max_sliceno; i++) {
        filename = slice_filename(eg->segfile->filename, i);
        fd = dv_open_file(filename, flags, SEGMENT_FILE_MODE);
        if (fd < 0) {
            ereport(ERROR, (errmsg("[file repair] open segment file failed, %s", filename)));
        }
        eg->segfile->segfiles[i].fd = fd;
        eg->segfile->segfiles[i].sliceno = i;
        pfree(filename);
    }

    int maxopenslicefd = eg->segfile->segfiles[max_sliceno].fd;
    eg->segfile->file_num = max_sliceno + 1;
    off_t size = lseek(maxopenslicefd, 0L, SEEK_END);
    if (max_sliceno == 0) {
        eg->segfile->total_blocks = size;
    } else {
        eg->segfile->total_blocks = max_sliceno * RELSEG_SIZE + size / BLCKSZ;
    }

    file_lock.unLock();
    eg_lock.unLock();
    spc_lock.unLock();
    return;
}

/*
 * sliceno == 0, means opening all files; otherwises open until the target slice.
 */
static void df_open_target_files(SegLogicFile *sf, int targetno)
{
    int sliceno = sf->file_num;
    uint32 flags = O_RDWR | PG_BINARY;

    for (;;) {
        if (targetno != 0 && targetno < sliceno) {
            break;
        }

        char *filename = slice_filename(sf->filename, sliceno);
        if (sliceno >= sf->vector_capacity) {
            df_extend_file_vector(sf);
        }
        int fd = dv_open_file(filename, flags, SEGMENT_FILE_MODE);
        if (fd < 0) {
            if (errno != ENOENT) {
                ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", filename)));
            }
            // The file does not exist, break.
            ereport(LOG,
                    (errmodule(MOD_SEGMENT_PAGE), errmsg("File \"%s\" does not exist, stop read here.", filename)));
            pfree(filename);
            break;
        }

        sf->segfiles[sliceno].fd = fd;
        sf->segfiles[sliceno].sliceno = sliceno;

        off_t size = lseek(fd, 0L, SEEK_END);
        sf->total_blocks += size / BLCKSZ;

        pfree(filename);
        sliceno++;
        sf->file_num++;
    }
}

/*
 * Open all physical file slice for a segment logic file. It will open these slices one bye one until
 * the open system call returning ENOENT.
 */
void df_open_files(SegLogicFile *sf)
{
    SegmentCheck(sf->segfiles == NULL);
    SegmentCheck(sf->file_num == 0);
    SegmentCheck(sf->vector_capacity == 0);
    SegmentCheck(sf->total_blocks == 0);

    df_open_target_files(sf, 0);
}

/*
 * Extend logic file once. Each time we extend at most DF_FILE_SLICE_SIZE.
 */
void df_extend_internal(SegLogicFile *sf)
{
    int fd = sf->segfiles[sf->file_num - 1].fd;

    off_t last_file_size = lseek(fd, 0L, SEEK_END);
    SegmentCheck(last_file_size <= DF_FILE_SLICE_SIZE);

    if (last_file_size == DF_FILE_SLICE_SIZE) {
        int new_sliceno = sf->file_num;
        char *filename = slice_filename(sf->filename, new_sliceno);
        if (new_sliceno >= sf->vector_capacity) {
            df_extend_file_vector(sf);
        }
        int new_fd = dv_open_file(filename, O_RDWR | O_CREAT, SEGMENT_FILE_MODE);
        if (new_fd < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("[segpage] could not create file \"%s\": %m", filename)));
        }

        if (ftruncate(new_fd, DF_FILE_EXTEND_STEP_SIZE) != 0) {
            dv_close_file(new_fd);
            ereport(ERROR,
                (errmodule(MOD_SEGMENT_PAGE),
                    errcode_for_file_access(),
                    errmsg("ftuncate file %s failed during df_extend due to %s", filename, strerror(errno)),
                    errdetail("file path: %s", sf->filename)));
        }

        sf->segfiles[new_sliceno] = {.fd = new_fd, .sliceno = new_sliceno};
        sf->file_num++;
        sf->total_blocks += DF_FILE_EXTEND_STEP_BLOCKS;
        pfree(filename);
    } else {
        ssize_t new_size;
        if (last_file_size % DF_FILE_EXTEND_STEP_SIZE == 0) {
            new_size = last_file_size + DF_FILE_EXTEND_STEP_SIZE;
        } else {
            new_size = CM_ALIGN_ANY(last_file_size, DF_FILE_EXTEND_STEP_SIZE);
        }

        SegmentCheck(new_size <= DF_FILE_SLICE_SIZE);

        if (ftruncate(fd, new_size) != 0) {
            char *filename = slice_filename(sf->filename, sf->file_num - 1);
            ereport(ERROR, (errmsg("ftuncate file %s failed during df_extend due to %s", filename, strerror(errno))));
        }

        sf->total_blocks += (new_size - last_file_size) / BLCKSZ;
    }
}

/*
 * Extend a logic file to target blocks.
 */
void df_extend(SegLogicFile *sf, BlockNumber target_blocks)
{
    if (target_blocks == InvalidBlockNumber) {
        SegmentCheck(0);
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("df_extend target_blocks is InvalidBlockNumber!\n")));
    }

    /* align target blocks to DF_FILE_EXTEND_STEP_BLOCKS */
    target_blocks = CM_ALIGN_ANY(target_blocks, DF_FILE_EXTEND_STEP_BLOCKS);

    AutoMutexLock lock(&sf->filelock);
    lock.lock();

    if (sf->total_blocks < target_blocks) {
        if (!RecoveryInProgress()) {
            uint64 requestSize = 1LLU * BLCKSZ * (target_blocks - sf->total_blocks);
            TableSpaceUsageManager::IsExceedMaxsize(sf->relNode.spcNode, requestSize, true);
        }
    } else {
        return;
    }

    SegmentCheck(sf->file_num > 0);

    while (sf->total_blocks < target_blocks) {
        df_extend_internal(sf);
    }
    ereport(LOG, (errmsg("extend data file %s to %u blocks", sf->filename, target_blocks)));
}

void df_shrink(SegLogicFile *sf, BlockNumber target)
{
    AutoMutexLock lock(&sf->filelock);
    lock.lock();
    ssize_t last_file_size;
    ssize_t last_file_blocks;
    BlockNumber shrink_blocks;

    SegmentCheck(target % DF_FILE_EXTEND_STEP_BLOCKS == 0);
    if (sf->total_blocks < target) {
        return;
    }

    ereport(LOG, (errmodule(MOD_SEGMENT_PAGE),
                  errmsg("df shrink %s, current blocks %u, target blocks %u", sf->filename, sf->total_blocks, target)));

    for (int i = sf->file_num - 1; sf->total_blocks > target && i >= 0; i--) {
        int fd = sf->segfiles[i].fd;
        last_file_size = lseek(fd, 0L, SEEK_END);
        last_file_blocks = last_file_size / BLCKSZ;
        shrink_blocks = 0;

        char *filename = slice_filename(sf->filename, sf->segfiles[i].sliceno);

        if (last_file_blocks + target <= sf->total_blocks) {
            // we need delete the last file and will continue the shrink
            shrink_blocks = last_file_blocks;
            dv_close_file(fd);
            sf->segfiles[i].fd = -1;

            seg_register_forget_request(sf, i);

            if (unlink(filename) != 0) {
                /* The fd is closed, if we can not open it, next access to this file will panic */
                uint32 flags = O_RDWR | O_EXCL | PG_BINARY;
                sf->segfiles[i].fd = dv_open_file(filename, flags, SEGMENT_FILE_MODE);
                if (sf->segfiles[i].fd < 0) {
                    ereport(PANIC, (errmsg("Unlink file %s failed and unable to read it again.", filename)));
                } else {
                    ereport(
                        ERROR, (errmsg("unlink file %s failed during df_shrink due to %s", filename, strerror(errno))));
                }
            }
            sf->file_num--;
        } else {
            // we need shrink the last file and finish the shrink
            shrink_blocks = sf->total_blocks - target;
            ssize_t new_size = last_file_size - (ssize_t)(shrink_blocks)*BLCKSZ;
            if (ftruncate(fd, new_size) != 0) {
                ereport(ERROR,
                        (errmsg("ftuncate file %s failed during df_shrink due to %s", filename, strerror(errno))));
            }
        }
        pfree(filename);
        sf->total_blocks -= shrink_blocks;
    }
    SegmentCheck(sf->total_blocks == target);
}

/*
 * Data file layer
 *
 * This layer provides a logic file interface. Invokers can use it as if there is just one file
 * which is in fact divided into a series of physical files (called file slices here) to avoid single
 * file exceeds the file system's limit.
 */
void df_pread_block(SegLogicFile *sf, char *buffer, BlockNumber blocknum)
{
    off_t offset = ((off_t)blocknum) * BLCKSZ;
    int sliceno = DF_OFFSET_TO_SLICENO(offset);
    off_t roffset = DF_OFFSET_TO_SLICE_OFFSET(offset);

    pgstat_report_waitevent(WAIT_EVENT_DATA_FILE_READ);
    SegPhysicalFile spf = df_get_physical_file(sf, sliceno, blocknum);
    int nbytes = pread(spf.fd, buffer, BLCKSZ, roffset);
    pgstat_report_waitevent(WAIT_EVENT_END);
    if (nbytes != BLCKSZ) {
        ereport(ERROR,
            (errcode(MOD_SEGMENT_PAGE),
                errcode_for_file_access(),
                errmsg("could not read segment block %d in file %s", blocknum, sf->filename),
                errdetail("errno: %d", errno)));
    }
}

void df_pwrite_block(SegLogicFile *sf, const char *buffer, BlockNumber blocknum)
{
    off_t offset = ((off_t)blocknum) * BLCKSZ;
    int sliceno = DF_OFFSET_TO_SLICENO(offset);
    off_t roffset = DF_OFFSET_TO_SLICE_OFFSET(offset);

    pgstat_report_waitevent(WAIT_EVENT_DATA_FILE_WRITE);
    SegPhysicalFile spf = df_get_physical_file(sf, sliceno, blocknum);
    int nbytes = pwrite(spf.fd, buffer, BLCKSZ, roffset);
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (nbytes != BLCKSZ) {
        ereport(ERROR,
            (errcode(MOD_SEGMENT_PAGE),
                errcode_for_file_access(),
                errmsg("could not write segment block %d in file %s, ", blocknum, sf->filename),
                errdetail("errno: %d", errno)));
    }

    seg_register_dirty_file(sf, sliceno);
}

void df_ctrl_init(SegLogicFile *sf, RelFileNode relNode, ForkNumber forknum)
{
    sf->file_num = 0;
    sf->segfiles = NULL;
    sf->vector_capacity = 0;
    sf->total_blocks = 0;

    sf->relNode = relNode;
    sf->forknum = forknum;

    char *filename = relpathperm(relNode, forknum);
    errno_t er = strcpy_s(sf->filename, MAXPGPATH, filename);
    securec_check(er, "\0", "\0");
    pfree(filename);

    pthread_mutex_init(&sf->filelock, NULL);
}

void df_fsync(SegLogicFile *sf)
{
    AutoMutexLock filelock(&sf->filelock);
    filelock.lock();
    for (int i = 0; i < sf->file_num; i++) {
        if (pg_fsync(sf->segfiles[i].fd)) {
            ereport(data_sync_elevel(ERROR), (errcode_for_file_access(), errmodule(MOD_SEGMENT_PAGE),
                errmsg("recover failed could not fsync segment data file"),
                errdetail( "filename \"%s\", slot id %d, error message: %m", sf->filename, i)));
        }
    }
}

/*
 * Unlink all **OPENED** data files of the given segment logic file. Thus the logic file must
 * be opened before invoking this function. If the first slice is unlinked, the unlink procedure
 * is considered as successful. The rest slices will be covered if we create a new logic file
 * with the same filenode.
 */
void df_unlink(SegLogicFile *sf)
{
    AutoMutexLock filelock(&sf->filelock);
    filelock.lock();

    /*
     * We first record "drop tablespace" xlog, then do the unlink. When the recovery thread 
     * replay the drop tablespace xlog, it will reopen the space, i.e., finding all data files
     * from 0 to n. Thus, we should unlink files from back to front in case of failure happens 
     * on half, so that the recovery thread can still unlink all files.
     */
    for (int i = sf->file_num-1; i >= 0; i--) {
        dv_close_file(sf->segfiles[i].fd);
        sf->segfiles[i].fd = -1;

        char *path = slice_filename(sf->filename, i);
        int ret = unlink(path);
        pfree(path);
        if (ret < 0) {
            ereport(ERROR, (errmsg("Could not remove file %s", path)));
        }
        sf->file_num--;
    }
    sf->file_num = 0;
}

/* fsync related API */
void forget_space_fsync_request(SegSpace *spc)
{
    for (int i = 0; i < (int)EXTENT_GROUPS; i++) {
        for (int j = 0; j <= SEGMENT_MAX_FORKNUM; j++) {
            SegLogicFile *sf = spc->extent_group[i][j].segfile;
            if (sf != NULL) {
                for (int k = 0; k < sf->file_num; k++) {
                    seg_register_forget_request(sf, k);
                }
            }
        }
    }
}

void seg_register_forget_request(SegLogicFile *sf, int segno)
{
    FileTag ftag;
    errno_t er = memset_s(&ftag, sizeof(FileTag), 0, sizeof(FileTag));
    securec_check(er, "", "");

    ftag.rnode = sf->relNode;
    ftag.forknum = sf->forknum;
    ftag.handler = SYNC_HANDLER_SEGMENT;
    ftag.segno = segno;

    RegisterSyncRequest(&ftag, SYNC_FORGET_REQUEST, true /* retryOnError */);
}

void seg_register_dirty_file(SegLogicFile *sf, int segno)
{
    FileTag ftag;
    errno_t er = memset_s(&ftag, sizeof(FileTag), 0, sizeof(FileTag));
    securec_check(er, "", "");

    /* Initialize ftag */
    ftag.rnode = sf->relNode;
    ftag.forknum = sf->forknum;
    ftag.rnode.bucketNode = SegmentBktId;
    ftag.handler = SYNC_HANDLER_SEGMENT;
    ftag.segno = segno;

    if (!RegisterSyncRequest(&ftag, SYNC_REQUEST, false)) {
        ereport(DEBUG5,
                (errmodule(MOD_SEGMENT_PAGE), errmsg("could not forward fsync request because request queue is full")));

        if (pg_fsync(sf->segfiles[segno].fd) < 0) {
            char *filename = slice_filename(sf->filename, segno);
            ereport(data_sync_elevel(ERROR),
                    (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", filename)));
            pfree(filename);
        }
    }
}

int seg_sync_filetag(const FileTag *ftag, char *path)
{
    /* Open tablespace */
    SegSpace *spc = spc_open(ftag->rnode.spcNode, ftag->rnode.dbNode, false);
    if (spc == NULL) {
        ereport(LOG, (errmodule(MOD_SEGMENT_PAGE),
                      errmsg("[segpage] seg_sync_filetag %s, ftag->segno is %u, but space is empty",
                             relpathperm(ftag->rnode, ftag->forknum), ftag->segno)));
        errno = ENOENT;
        return -1;
    }
    int extent_size = EXTENT_TYPE_TO_SIZE(ftag->rnode.relNode);
    int egid = EXTENT_SIZE_TO_GROUPID(extent_size);

    SegLogicFile *lf = spc->extent_group[egid][ftag->forknum].segfile;
    strlcpy(path, lf->filename, MAXPGPATH);

    AutoMutexLock lock(&lf->filelock);
    lock.lock();

    if (ftag->segno >= (uint32)lf->file_num) {
        /* File may be deleted by spc_shrink or space delete */
        ereport(LOG,
            (errmodule(MOD_SEGMENT_PAGE),
                errmsg("[segpage] seg_sync_filetag %s, ftag->segno is %u, but existing file number is %u, the file may "
                       "be deleted by spc_shrink or drop tablespace",
                    path, ftag->segno, lf->file_num)));
        errno = ENOENT;
        return -1;
    }

    /* can not sync the file not exist */
    SegPhysicalFile *sf = &lf->segfiles[ftag->segno];

    SegmentCheck((uint32)sf->sliceno == ftag->segno);
    int ret = pg_fsync(sf->fd);

    ereport(DEBUG1, (errmsg("segment fsync %s", path)));

    return ret;
}

int seg_unlink_filetag(const FileTag *ftag, char *path)
{
    SegmentCheck(0);
    return 0;
}

void segForgetDatabaseFsyncRequests(Oid dbid)
{
    FileTag tag;
    RelFileNode rnode = {.spcNode = 0, .dbNode = dbid, .relNode = 0, .bucketNode = InvalidBktId, .opt = 0};

    tag.rnode = rnode;
    tag.handler = SYNC_HANDLER_SEGMENT;
    tag.forknum = InvalidForkNumber;
    tag.segno = InvalidBlockNumber;

    RegisterSyncRequest(&tag, SYNC_FILTER_REQUEST, true /*retry on error */);
}

bool seg_filetag_matches(const FileTag *ftag, const FileTag *candidate)
{
    return ftag->rnode.dbNode == candidate->rnode.dbNode;
}
