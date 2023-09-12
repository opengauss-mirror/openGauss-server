/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * storage_exrto_file.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/storage/smgr/storage_exrto_file.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "storage/smgr/fd.h"
#include "storage/vfd.h"
#include "storage/smgr/smgr.h"
#include "utils/memutils.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"
#include "access/extreme_rto/standby_read/block_info_meta.h"

const uint32 EXRTO_BASE_PAGE_FILE_BLOCKS = EXRTO_BASE_PAGE_FILE_MAXSIZE / BLCKSZ;
const uint32 EXRTO_LSN_INFO_FILE_BLOCKS = EXRTO_LSN_INFO_FILE_MAXSIZE / BLCKSZ;
const uint32 EXRTO_BLOCK_INFO_FILE_BLOCKS = RELSEG_SIZE;

const int EXTEND_BLOCKS_NUM = 16;
const uint64 EXRTO_INVALID_BLOCK_NUMBER = 0xFFFFFFFFFFFFFFFFL;

const uint32 EXRTO_FILE_SIZE[] = {
    EXRTO_BASE_PAGE_FILE_MAXSIZE, EXRTO_LSN_INFO_FILE_MAXSIZE, EXRTO_BLOCK_INFO_FILE_MAXSIZE};
const uint32 EXRTO_FILE_BLOCKS[] = {
    EXRTO_BASE_PAGE_FILE_BLOCKS, EXRTO_LSN_INFO_FILE_BLOCKS, EXRTO_BLOCK_INFO_FILE_BLOCKS};

typedef struct _ExRTOFileState {
    uint64 segno[EXRTO_FORK_NUM];
    File file[EXRTO_FORK_NUM];
} ExRTOFileState;

static inline void set_file_state(ExRTOFileState *state, ForkNumber forknum, uint64 segno, File file)
{
    state->segno[forknum] = segno;
    state->file[forknum] = file;
}

static ExRTOFileState *alloc_file_state(void)
{
    MemoryContext current;
    ExRTOFileState *state;
    if (EnableLocalSysCache()) {
        current = t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;
    } else {
        current = u_sess->storage_cxt.exrto_standby_read_file_cxt;
    }
    state = (ExRTOFileState *)MemoryContextAllocZero(current, sizeof(ExRTOFileState));
    for (int i = 0; i < EXRTO_FORK_NUM; i++) {
        state->file[i] = -1;
    }

    return state;
}

static void exrto_get_file_path(const RelFileNode node, ForkNumber forknum, uint64 segno, char *path)
{
    ExRTOFileType type;
    char filename[EXRTO_FILE_PATH_LEN];
    errno_t rc = EOK;

    type = exrto_file_type(node.spcNode);
    if (type == BASE_PAGE || type == LSN_INFO_META) {
        uint32 batch_id = node.dbNode >> LOW_WORKERID_BITS;
        uint32 worker_id = node.dbNode & LOW_WORKERID_MASK;
        rc = snprintf_s(filename, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%02x%02x%016lX",
            batch_id, worker_id, segno);
    } else {
        rc = snprintf_s(filename, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%u_%u_%s.%u",
            node.dbNode, node.relNode, forkNames[forknum], (uint32)segno);
    }
    securec_check_ss(rc, "\0", "\0");

    rc = snprintf_s(path, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%s/%s/%s",
        EXRTO_FILE_DIR, EXRTO_FILE_SUB_DIR[type], filename);
    securec_check_ss(rc, "\0", "\0");

    return;
}

static uint64 get_seg_num(const RelFileNodeBackend& smgr_rnode, BlockNumber blocknum)
{
    ExRTOFileType type;
    uint32 blocks_per_file;
    uint64 total_blocknum;
    uint64 segno;

    type = exrto_file_type(smgr_rnode.node.spcNode);
    blocks_per_file = EXRTO_FILE_BLOCKS[type];
    total_blocknum = get_total_block_num(type, smgr_rnode.node.relNode, blocknum);
    segno = (total_blocknum / blocks_per_file);

    return segno;
}

static RelFileNodeForkNum exrto_file_relfilenode_forknum_fill(const RelFileNodeBackend &rnode,
                                                              ForkNumber forknum, uint64 segno)
{
    RelFileNodeForkNum node;
    ExRTOFileType type;

    errno_t rc = memset_s(&node, sizeof(RelFileNodeForkNum), 0, sizeof(RelFileNodeForkNum));
    securec_check(rc, "", "");
    node.rnode = rnode;
    type = exrto_file_type(rnode.node.spcNode);
    if (type == BASE_PAGE || type == LSN_INFO_META) {
        node.rnode.node.relNode = segno >> UINT64_HALF;
    }
    node.forknumber = forknum;
    node.segno = (uint32)segno;
    node.storage = ROW_STORE;

    return node;
}

static ExRTOFileState *exrto_open_file(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                                       ExtensionBehavior behavior)
{
    ExRTOFileState* state = (ExRTOFileState *)reln->fileState;
    uint64 segno;
    uint32 flags = O_RDWR | PG_BINARY;
    char file_path[EXRTO_FILE_PATH_LEN];
    RelFileNodeForkNum filenode;
    File fd;

    segno = get_seg_num(reln->smgr_rnode, blocknum);
    /* No work if already open */
    if (state != NULL) {
        if (state->file[forknum] > 0) {
            if (state->segno[forknum] == segno) {
                return state;
            }
            /* This is not the file we're looking for. */
            FileClose(state->file[forknum]);
        }
    } else {
        state = alloc_file_state();
        reln->fileState = state;
    }
    set_file_state(state, forknum, 0, -1);

    if (behavior == EXTENSION_CREATE) {
        flags |= O_CREAT;
    }
    ADIO_RUN() {
        flags |= O_DIRECT;
    }
    ADIO_END();

    exrto_get_file_path(reln->smgr_rnode.node, forknum, segno, file_path);
    filenode = exrto_file_relfilenode_forknum_fill(reln->smgr_rnode, forknum, segno);
    fd = DataFileIdOpenFile(file_path, filenode, (int)flags, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        if ((behavior == EXTENSION_RETURN_NULL) && FILE_POSSIBLY_DELETED(errno)) {
            return NULL;
        }
        exrto_close(reln, forknum, InvalidBlockNumber);
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", file_path)));
    }

    set_file_state(state, forknum, segno, fd);

    return state;
}
 
bool exrto_check_unlink_relfilenode(const RelFileNode rnode)
{
    HTAB *relfilenode_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
    bool found = false;
 
    LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_SHARED);
    (void)hash_search(relfilenode_hashtbl, &(rnode), HASH_FIND, &found);
    LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);
 
    return found;
}

BlockNumber get_single_file_nblocks(SMgrRelation reln, ForkNumber forknum, const ExRTOFileState *state)
{
    Assert(state != NULL);

    char *filename = FilePathName(state->file[forknum]);
    off_t len = FileSeek(state->file[forknum], 0L, SEEK_END);
    if (len < 0) {
        char filepath[EXRTO_FILE_PATH_LEN];
        errno_t rc = strcpy_s(filepath, EXRTO_FILE_PATH_LEN, filename);
        securec_check(rc, "\0", "\0");
        exrto_close(reln, forknum, InvalidBlockNumber);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek to end of file \"%s\": %m", filepath)));
    }

    /* note that this calculation will ignore any partial block at EOF */
    return (BlockNumber)(len / BLCKSZ);
}

void exrto_init(void)
{
    if (EnableLocalSysCache()) {
        return;
    }
    Assert(u_sess->storage_cxt.exrto_standby_read_file_cxt == NULL);
    u_sess->storage_cxt.exrto_standby_read_file_cxt =
        AllocSetContextCreate(u_sess->top_mem_cxt, "ExrtoFileSmgr", ALLOCSET_DEFAULT_SIZES);
}

void exrto_close(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
    ExRTOFileState* state = (ExRTOFileState*)reln->fileState;
 
    /* No work if already closed */
    if (state == NULL) {
        return;
    }

    /* if not closed already */
    if (state->file[forknum] >= 0) {
        FileClose(state->file[forknum]);
        state->file[forknum] = -1;
    }
    for (int forkno = 0; forkno < EXRTO_FORK_NUM; forkno++) {
        if (state->file[forkno] != -1) {
            return;
        }
    }

    pfree(state);
    reln->fileState = NULL;
}

bool exrto_exists(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
    /*
     * Close it first, to ensure that we notice if the fork has been unlinked
     * since we opened it.
     */
    exrto_close(reln, forknum, blocknum);

    bool isExist = false;
    if (exrto_open_file(reln, forknum, blocknum, EXTENSION_RETURN_NULL) != NULL) {
        isExist = true;
    }
    exrto_close(reln, forknum, blocknum);
    return isExist;
}

void exrto_unlink_file_with_prefix(char* target_prefix, ExRTOFileType type, uint64 segno)
{
    char pathbuf[EXRTO_FILE_PATH_LEN];
    char **filenames;
    char **filename;
    struct stat statbuf;
    /* get file directory */
    char exrto_block_info_dir[EXRTO_FILE_PATH_LEN] = {0};
    int rc = snprintf_s(exrto_block_info_dir, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%s/%s", EXRTO_FILE_DIR,
                        EXRTO_FILE_SUB_DIR[type]);
    securec_check_ss(rc, "", "");
    /* get all files' name from block meta file directory */
    filenames = pgfnames(exrto_block_info_dir);
    if (filenames == NULL) {
        return;
    }

    /* use the prefix name to match up files we want to delete */
    size_t prefix_len = strlen(target_prefix);
    for (filename = filenames; *filename != NULL; filename++) {
        char *fname = *filename;
        size_t fname_len = strlen(fname);
        /*
         * the length of prefix is less than the length of file name and must be the same under the same prefix_len
         */
        if (prefix_len >= fname_len || strncmp(target_prefix, fname, prefix_len) != 0) {
            continue;
        }
        if (segno > 0) {
            uint32 batch_id, worker_id;
            uint64 f_segno;
            const int para_num = 3;
            if (sscanf_s(fname, "%02X%02X%016lX", &batch_id, &worker_id, &f_segno) != para_num) {
                continue;
            }
            if (f_segno >= segno) {
                continue;
            }
        }

        rc =
            snprintf_s(pathbuf, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%s/%s", exrto_block_info_dir, *filename);
        securec_check_ss(rc, "", "");
        /* may be can be some error */
        if (lstat(pathbuf, &statbuf) != 0) {
            if (errno != ENOENT) {
                ereport(WARNING, (errmsg("could not stat file or directory \"%s\" \n", pathbuf)));
            }
            continue;
        }
        /* if the file is a directory, don't touch it */
        if (S_ISDIR(statbuf.st_mode)) {
            /* skip dir */
            continue;
        }
        /* delete this file we found */
        if (unlink(pathbuf) != 0) {
            if (errno != ENOENT) {
                ereport(WARNING, (errmsg("could not remove file or directory \"%s\" ", pathbuf)));
            }
        }
    }
    pgfnames_cleanup(filenames);
    return;
}

void exrto_unlink(const RelFileNodeBackend &rnode, ForkNumber forknum, bool is_redo, BlockNumber blocknum)
{
    char target_prefix[EXRTO_FILE_PATH_LEN] = {0};
    ExRTOFileType type = exrto_file_type(rnode.node.spcNode);
    uint64 segno;
    errno_t rc;

    if (type == BLOCK_INFO_META) {
        /* unlink all files */
        rc = sprintf_s(target_prefix, EXRTO_FILE_PATH_LEN, "%u_%u_", rnode.node.dbNode, rnode.node.relNode);
        securec_check_ss(rc, "", "");
        exrto_unlink_file_with_prefix(target_prefix, type);
    } else if (type == BASE_PAGE || type == LSN_INFO_META) {
        /* just unlink the files before the file where blocknum is */
        segno = get_seg_num(rnode, blocknum);
        if (segno > 0) {
            uint32 batch_id = rnode.node.dbNode >> LOW_WORKERID_BITS;
            uint32 worker_id = rnode.node.dbNode & LOW_WORKERID_MASK;
            rc = sprintf_s(target_prefix, EXRTO_FILE_PATH_LEN, "%02X%02X", batch_id, worker_id);
            securec_check_ss(rc, "", "");
            exrto_unlink_file_with_prefix(target_prefix, type, segno);
        }
    }
}

/* extend EXTEND_BLOCKS_NUM pages */
void exrto_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skip_fsync)
{
    ExRTOFileState *state = NULL;
    ExRTOFileType type;
    uint64 total_block_num;
    off_t seekpos;
    int nbytes;
    struct stat file_stat;
    char* filename;
    ExtensionBehavior behavior;

    type = exrto_file_type(reln->smgr_rnode.node.spcNode);
    total_block_num = get_total_block_num(type, reln->smgr_rnode.node.relNode, blocknum);
    if (total_block_num == EXRTO_INVALID_BLOCK_NUMBER) {
        ereport(ERROR,
            (errmsg("cannot extend file beyond %lu blocks.", EXRTO_INVALID_BLOCK_NUMBER)));
    }
    seekpos = (off_t)BLCKSZ * (total_block_num % EXRTO_FILE_BLOCKS[type]);

    behavior = (type == BLOCK_INFO_META ? EXTENSION_RETURN_NULL : EXTENSION_CREATE);
    state = exrto_open_file(reln, forknum, blocknum, behavior);
    if (state == NULL) {
        Assert(type == BLOCK_INFO_META);
        if (exrto_check_unlink_relfilenode(reln->smgr_rnode.node)) {
            return;
        } else {
            state = exrto_open_file(reln, forknum, blocknum, EXTENSION_CREATE);
        }
    }
    filename = FilePathName(state->file[forknum]);
    if (stat(filename, &file_stat) < 0) {
        char filepath[EXRTO_FILE_PATH_LEN];
        errno_t rc = strcpy_s(filepath, EXRTO_FILE_PATH_LEN, filename);
        securec_check(rc, "\0", "\0");
        exrto_close(reln, forknum, InvalidBlockNumber);
        ereport(ERROR, (errmsg("could not stat file \"%s\": %m.", filepath)));
    }
    Assert(file_stat.st_size % BLCKSZ == 0);
    Assert(file_stat.st_size <= EXRTO_FILE_SIZE[type]);

    if (seekpos < file_stat.st_size) {
        /* no need to extend */
        return;
    }

    int extend_size = rtl::min(rtl::max(EXTEND_BLOCKS_NUM * BLCKSZ, (int)((seekpos - file_stat.st_size) + BLCKSZ)),
        (int)(EXRTO_FILE_SIZE[type] - file_stat.st_size));
    nbytes = FilePWrite(state->file[forknum], NULL, extend_size, file_stat.st_size);
    if (nbytes != extend_size) {
        char filepath[EXRTO_FILE_PATH_LEN];
        errno_t rc = strcpy_s(filepath, EXRTO_FILE_PATH_LEN, filename);
        securec_check(rc, "\0", "\0");
        exrto_close(reln, forknum, InvalidBlockNumber);
        if (nbytes < 0) {
            ereport(ERROR, (errmsg("could not extend file \"%s\": %m.", filepath)));
        }
        ereport(ERROR,
            (errmsg("could not extend file \"%s\": wrote only %d of %d bytes.", filepath, nbytes, extend_size)));
    }

    Assert(get_single_file_nblocks(reln, forknum, state) <= ((BlockNumber)EXRTO_FILE_BLOCKS[type]));
}

SMGR_READ_STATUS exrto_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer)
{
    ExRTOFileState *state = NULL;
    ExRTOFileType type;
    ExtensionBehavior behavior;
    uint64 total_block_num;
    off_t seekpos;
    int nbytes;
    errno_t rc;

    type = exrto_file_type(reln->smgr_rnode.node.spcNode);
    if (type == LSN_INFO_META || type == BLOCK_INFO_META) {
        behavior = EXTENSION_RETURN_NULL;
    } else {
        behavior = EXTENSION_FAIL;
    }

    total_block_num = get_total_block_num(type, reln->smgr_rnode.node.relNode, blocknum);
    if (total_block_num == EXRTO_INVALID_BLOCK_NUMBER) {
        ereport(ERROR,
            (errmsg("cannot read file beyond %lu blocks.", EXRTO_INVALID_BLOCK_NUMBER)));
    }
    seekpos = (off_t)BLCKSZ * (total_block_num % EXRTO_FILE_BLOCKS[type]);

    state = exrto_open_file(reln, forknum, blocknum, behavior);
    if (state == NULL) {
        /* For lsn info and block info page, just set buffer to all zeros when not found on disk. */
        rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");
        return SMGR_RD_OK;
    }

    nbytes = FilePRead(state->file[forknum], buffer, BLCKSZ, seekpos);
    if (nbytes == 0 && (type == LSN_INFO_META || type == BLOCK_INFO_META)) {
        rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");
        return SMGR_RD_OK;
    }
    if (nbytes != BLCKSZ) {
        char *filename = FilePathName(state->file[forknum]);
        char filepath[EXRTO_FILE_PATH_LEN];
        rc = strcpy_s(filepath, EXRTO_FILE_PATH_LEN, filename);
        securec_check(rc, "\0", "\0");
        exrto_close(reln, forknum, InvalidBlockNumber);
        if (nbytes < 0) {
            ereport(ERROR, (errmsg("could not read block %u in file \"%s\": %m.", blocknum, filepath)));
        }
        ereport(ERROR, (errmsg("could not read block %u in file \"%s\": read only %d of %d bytes.", blocknum, filepath,
                               nbytes, BLCKSZ)));
    }

    if (PageIsVerified((Page)buffer, blocknum)) {
        return SMGR_RD_OK;
    } else {
        return SMGR_RD_CRC_ERROR;
    }
}

void exrto_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skip_fsync)
{
    ExRTOFileState *state = NULL;
    ExRTOFileType type;
    uint64 total_block_num;
    off_t seekpos;
    int nbytes;
    ExtensionBehavior behavior;

    type = exrto_file_type(reln->smgr_rnode.node.spcNode);
    total_block_num = get_total_block_num(type, reln->smgr_rnode.node.relNode, blocknum);
    if (total_block_num == EXRTO_INVALID_BLOCK_NUMBER) {
        ereport(ERROR,
            (errmsg("cannot write file beyond %lu blocks.", EXRTO_INVALID_BLOCK_NUMBER)));
    }
    seekpos = (off_t)BLCKSZ * (total_block_num % EXRTO_FILE_BLOCKS[type]);

    Assert(seekpos < (off_t)EXRTO_FILE_SIZE[type]);

    behavior = (type == BLOCK_INFO_META ? EXTENSION_RETURN_NULL : EXTENSION_CREATE);
    state = exrto_open_file(reln, forknum, blocknum, behavior);
    if (state == NULL) {
        Assert(type == BLOCK_INFO_META);
        if (exrto_check_unlink_relfilenode(reln->smgr_rnode.node)) {
            return;
        } else {
            state = exrto_open_file(reln, forknum, blocknum, EXTENSION_CREATE);
        }
    }
    nbytes = FilePWrite(state->file[forknum], buffer, BLCKSZ, seekpos);
    if (nbytes != BLCKSZ) {
        char *filename = FilePathName(state->file[forknum]);
        char filepath[EXRTO_FILE_PATH_LEN];
        errno_t rc = strcpy_s(filepath, EXRTO_FILE_PATH_LEN, filename);
        securec_check(rc, "\0", "\0");
        exrto_close(reln, forknum, InvalidBlockNumber);
        if (nbytes < 0) {
            ereport(ERROR, (errmsg("could not write block %u in file \"%s\": %m.", blocknum, filepath)));
        }
        ereport(ERROR, (errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes.", blocknum,
                               filepath, nbytes, BLCKSZ)));
    }
}

BlockNumber exrto_nblocks(SMgrRelation, ForkNumber)
{
    return MaxBlockNumber;
}

void exrto_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
    ExRTOFileType type = exrto_file_type(reln->smgr_rnode.node.spcNode);
    Assert(type == BLOCK_INFO_META);

    BlockNumber curnblk = exrto_nblocks(reln, forknum);
    if (curnblk == 0) {
        return;
    }
    
    if (nblocks > curnblk) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
            relpath(reln->smgr_rnode, forknum), nblocks, curnblk)));
    }
    if (nblocks == curnblk) {
        return;
    }

    uint32 blocks_per_file = EXRTO_FILE_BLOCKS[type];
    for (BlockNumber prior_blocks = 0;; prior_blocks += blocks_per_file) {
        struct stat stat_buf;
        char segpath[EXRTO_FILE_PATH_LEN];
        uint64 segno = get_seg_num(reln->smgr_rnode, prior_blocks);
        exrto_get_file_path(reln->smgr_rnode.node, forknum, segno, segpath);
        if (stat(segpath, &stat_buf) < 0) {
            if (errno != ENOENT) {
                ereport(
                    WARNING,
                    (errcode_for_file_access(), errmsg("could not stat file \"%s\" before truncate: %m", segpath)));
            }
            break;
        }

        ExRTOFileState *state = exrto_open_file(reln, forknum, prior_blocks, EXTENSION_FAIL);
        if (prior_blocks > nblocks) {
            if (FileTruncate(state->file[forknum], 0) < 0) {
                ereport(DEBUG1,
                         (errcode_for_file_access(), errmsg("could not truncate file \"%s\": %m", segpath)));
            }
        } else if (prior_blocks + ((BlockNumber)blocks_per_file) > nblocks) {
            BlockNumber last_seg_block = nblocks - prior_blocks;
            off_t truncate_offset = (off_t)last_seg_block * BLCKSZ;

            if (FileTruncate(state->file[forknum], truncate_offset) < 0) {
                ereport(DEBUG1,
                         (errcode_for_file_access(), errmsg("could not truncate file \"%s\": %m", segpath)));
            }
        }
        exrto_close(reln, forknum, InvalidBlockNumber);
    }
}

void exrto_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks)
{
    ExRTOFileType type;
    uint64 total_block_num;
    type = exrto_file_type(reln->smgr_rnode.node.spcNode);
    total_block_num = get_total_block_num(type, reln->smgr_rnode.node.relNode, blocknum);
    ExtensionBehavior behavior = (type == BLOCK_INFO_META ? EXTENSION_RETURN_NULL : EXTENSION_CREATE);

    while (nblocks > 0) {
        BlockNumber nflush = nblocks;
        off_t seekpos;
        ExRTOFileState *state = NULL;
        uint64 segnum_start, segnum_end;
        state = exrto_open_file(reln, forknum, blocknum, behavior);
        if (state == NULL) {
            Assert(type == BLOCK_INFO_META);
            /* only check at first time */
            if (exrto_check_unlink_relfilenode(reln->smgr_rnode.node)) {
                return;
            } else {
                state = exrto_open_file(reln, forknum, blocknum, EXTENSION_CREATE);
            }
        }
        segnum_start = total_block_num / EXRTO_FILE_BLOCKS[type];
        segnum_end = (total_block_num + nblocks - 1) / EXRTO_FILE_BLOCKS[type];
 
        if (segnum_start != segnum_end) {
            nflush = EXRTO_FILE_BLOCKS[type] - (uint32)(total_block_num % EXRTO_FILE_BLOCKS[type]);
        }

        Assert(nflush >= 1);
        Assert(nflush <= nblocks);

        seekpos = (off_t)BLCKSZ * (total_block_num % EXRTO_FILE_BLOCKS[type]);
        FileWriteback(state->file[forknum], seekpos, (off_t)BLCKSZ * nflush);

        nblocks -= nflush;
        /* ensure that the relnode is not changed */
        Assert(((total_block_num + nflush) >> UINT64_HALF) == (total_block_num >> UINT64_HALF));
        total_block_num += nflush;
        blocknum = (BlockNumber)total_block_num;
        behavior = EXTENSION_CREATE;
    }
}
