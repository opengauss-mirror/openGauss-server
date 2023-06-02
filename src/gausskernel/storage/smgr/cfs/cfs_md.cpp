/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * -------------------------------------------------------------------------
 */
#include "storage/cfs/cfs.h"
#include "storage/cfs/cfs_converter.h"
#include "storage/cfs/cfs_md.h"
#include "storage/cfs/cfs_repair.h"
#include "storage/cfs/cfs_tools.h"
#include "storage/cfs/cfs_buffers.h"
#include "catalog/storage_xlog.h"
#include "storage/smgr/smgr.h"
#include "pgstat.h"
#include "postmaster/cfs_shrinker.h"

#define CFS_PCA_AND_ASSIST_BUFFER_SIZE (2 * CFS_EXTENT_SIZE * BLCKSZ)
#define CFS_WRITE_RETRY_TIMES 8

CfsLocationConvert cfsLocationConverts[2] = {
    StorageConvert,
    NULL
};

char* CfsHeaderPagerCheckStatusList[] = {"verified success.",
    "verified failed but repaired some values.",
    "verified failed."};

void CfsRecycleChunk(SMgrRelation reln, ForkNumber forknum);
void CfsShrinkerShmemListPush(const RelFileNode &rnode, ForkNumber forknum, char parttype);

int CfsReadPage(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, char *buffer,
                   CFS_STORAGE_TYPE type)
{
    errno_t rc;
    ExtentLocation location = cfsLocationConverts[type](reln, forknum, logicBlockNumber, false, EXTENT_OPEN_FILE);
    if (location.fd < 0) {
        return -1;
    }

    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");
        pca_buf_free_page(ctrl, location, false);

        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsReadPage %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
        return -1;
    }
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    CfsExtentAddress *cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)location.extentOffset);
    if (cfsExtentAddress->nchunks == 0) {
        rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");

        pca_buf_free_page(ctrl, location, false);
        return BLCKSZ;
    }

    RelFileCompressOption option;
    TransCompressOptions(reln->smgr_rnode.node, &option);

    auto chunkSize = CHUNK_SIZE_LIST[option.compressChunkSize];

    auto startOffset = location.extentStart * BLCKSZ;
    char *compressedBuffer = (char *) palloc(chunkSize * cfsExtentAddress->nchunks);
    char *bufferPos = compressedBuffer;

    for (auto i = 0; i < cfsExtentAddress->nchunks; i++) {
        bufferPos = compressedBuffer + (long)chunkSize * i;
        off_t seekPos = OffsetOfPageCompressChunk((uint16)chunkSize, cfsExtentAddress->chunknos[i]) + startOffset;
        uint8 start = (uint8)i;
        while (i < cfsExtentAddress->nchunks - 1 &&
               cfsExtentAddress->chunknos[i + 1] == cfsExtentAddress->chunknos[i] + 1) {
            i++;
        }
        int readAmount = (int)(chunkSize * ((int)(i - (int)start) + 1));
        int nbytes = FilePRead(location.fd, bufferPos, readAmount, seekPos, (uint32)WAIT_EVENT_DATA_FILE_READ);
        if (nbytes != readAmount) {
            rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
            securec_check(rc, "\0", "\0");

            pca_buf_free_page(ctrl, location, false);
            pfree(compressedBuffer);
            return -1;
        }
    }

    if (cfsExtentAddress->nchunks == (BLCKSZ / chunkSize)) {
        rc = memcpy_sp(buffer, BLCKSZ, compressedBuffer, BLCKSZ);
        securec_check(rc, "\0", "\0");
    } else if (DecompressPage(compressedBuffer, buffer) != BLCKSZ) {
        rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");

        pca_buf_free_page(ctrl, location, false);
        pfree(compressedBuffer);
        return -1;
    }

    pca_buf_free_page(ctrl, location, false);
    pfree(compressedBuffer);
    return BLCKSZ;
}

char *CfsCompressPage(const char *buffer, RelFileCompressOption *option, uint8 *nchunks)
{
    uint8 algorithm = option->compressAlgorithm;
    uint32 chunk_size = CHUNK_SIZE_LIST[option->compressChunkSize];
    auto work_buffer_size = CompressPageBufferBound(buffer, algorithm);
    int8 level = option->compressLevelSymbol ? option->compressLevel : -option->compressLevel;
    uint8 prealloc_chunk = option->compressPreallocChunks;

    if (work_buffer_size < 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("mdwrite_pc unrecognized compression algorithm %d,chunk_size:%d,level:%d,prealloc_chunk:%d",
                (int)algorithm, (int)chunk_size, level, (int)prealloc_chunk)));
    }

    char *work_buffer = (char *) palloc((unsigned long)work_buffer_size);
    auto compress_buffer_size = CompressPage(buffer, work_buffer, work_buffer_size, *option);
    if (compress_buffer_size < 0) {
        /* if error occurs, we don't compress, just store original page. */
        compress_buffer_size = BLCKSZ;
    }
    *nchunks = (uint8)((unsigned int)(compress_buffer_size - 1) / chunk_size + 1);
    auto bufferSize = chunk_size * (*nchunks);
    if (bufferSize >= BLCKSZ) {
        /* store original page if can not save space? */
        pfree(work_buffer);
        work_buffer = (char *) buffer;
        (*nchunks) = (uint8)(BLCKSZ / chunk_size);
    } else {
        /* fill zero in the last chunk */
        if ((uint32) compress_buffer_size < bufferSize) {
            auto leftSize = bufferSize - (uint32)compress_buffer_size;
            errno_t rc = memset_s(work_buffer + compress_buffer_size, leftSize, 0, leftSize);
            securec_check(rc, "", "");
        }
    }

    return work_buffer;
}

inline static uint4 PageCompressChunkSize(SMgrRelation reln)
{
    return CHUNK_SIZE_LIST[GET_COMPRESS_CHUNK_SIZE((reln)->smgr_rnode.node.opt)];
}

void CfsWriteBack(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks,
                  CFS_STORAGE_TYPE type)
{
    while (nblocks > 0) {
        ExtentLocation location = cfsLocationConverts[type](reln, forknum, blocknum, true, WRITE_BACK_OPEN_FILE);
        if (location.fd == -1) {
            return;
        }
        unsigned int segnum_start = blocknum / CFS_LOGIC_BLOCKS_PER_FILE;
        unsigned int segnum_end = (blocknum + nblocks - 1) / CFS_LOGIC_BLOCKS_PER_FILE;
        BlockNumber nflush = nblocks;
        if (segnum_start != segnum_end) {
            nflush = CFS_LOGIC_BLOCKS_PER_FILE - (blocknum % CFS_LOGIC_BLOCKS_PER_FILE);
        }
        off_t seekPos;
        for (BlockNumber iblock = 0; iblock < nflush; ++iblock) {
            uint32 chunkSize = PageCompressChunkSize(reln);
            location =
                cfsLocationConverts[type](reln, forknum, blocknum + iblock, true, WRITE_BACK_OPEN_FILE);

            pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
            if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
                pca_buf_free_page(ctrl, location, false);
                if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                    ereport(DEBUG1, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(
                            "could not write back %u in file \"%s\" headerNum: %u, relation has been removed", blocknum,
                            FilePathName(location.fd), location.headerNum)));
                    return;
                }
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Failed to CfsWriteBack %s, headerNum: %u.",
                                                                        FilePathName(location.fd),
                                                                        location.headerNum)));
            }
            CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

            CfsExtentAddress *cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)location.extentOffset);

            pc_chunk_number_t seekPosChunk;
            pc_chunk_number_t lastChunk;
            bool firstEnter = true;

            for (uint8 i = 0; i < cfsExtentAddress->nchunks; ++i) {
                if (firstEnter) {
                    seekPosChunk = cfsExtentAddress->chunknos[i];
                    lastChunk = seekPosChunk;
                    firstEnter = false;
                } else if (cfsExtentAddress->chunknos[i] == lastChunk + 1) {
                    lastChunk++;
                } else {
                    seekPos = OffsetOfPageCompressChunk((uint16)chunkSize, (int)seekPosChunk);
                    pc_chunk_number_t nchunks = (lastChunk - seekPosChunk) + 1;
                    FileWriteback(location.fd, seekPos, (off_t) nchunks * chunkSize);
                    seekPosChunk = cfsExtentAddress->chunknos[i];
                    lastChunk = seekPosChunk;
                }
            }
            /* flush the rest chunks */
            if (!firstEnter) {
                seekPos = (off_t) chunkSize * seekPosChunk;
                pc_chunk_number_t nchunks = (lastChunk - seekPosChunk) + 1;
                FileWriteback(location.fd, seekPos, (off_t) nchunks * chunkSize);
            }

            pca_buf_free_page(ctrl, location, false);
        }
        nblocks -= nflush;
        blocknum += nflush;
    }
}

// return bool indicate that the extentheader is changed
static inline bool ExtendChunksOfBlockCore(CfsExtentHeader *cfsExtentHeader, CfsExtentAddress *cfsExtentAddress,
                                           uint8 needChunks, uint8 actualUse)
{
    bool res = false;
    if (cfsExtentAddress->allocated_chunks < needChunks) {
        /* since chunks is to be allocated, it means the order may break down
            the ext will be dealt with in chunk recycling
            recycleInOrder means whether the ext is in order or not
         */
        cfsExtentHeader->recycleInOrder = 0;
        auto allocateNumber = needChunks - cfsExtentAddress->allocated_chunks;
        uint32 chunkno = (pc_chunk_number_t)pg_atomic_fetch_add_u32(&cfsExtentHeader->allocated_chunks,
                                                                    (uint32)allocateNumber) + 1;
        for (int i = cfsExtentAddress->allocated_chunks; i < needChunks; ++i, ++chunkno) {
            cfsExtentAddress->chunknos[i] = (uint16)chunkno;
        }
        cfsExtentAddress->allocated_chunks = needChunks;
        res = true;
    }
    if (cfsExtentAddress->nchunks != actualUse) {
        cfsExtentAddress->nchunks = actualUse;
        res = true;
    }
    uint32 cksm = AddrChecksum32(cfsExtentAddress, cfsExtentAddress->allocated_chunks);
    if (cfsExtentAddress->checksum != cksm) {
        cfsExtentAddress->checksum = cksm;
        res = true;
    }
    return res;
}

static inline bool ExtendChunksOfBlock(CfsExtentHeader *cfsExtentHeader, ExtentLocation *location, uint8 needChunks,
                                       uint8 actualUse)
{
    auto cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)location->extentOffset);
    return ExtendChunksOfBlockCore(cfsExtentHeader, cfsExtentAddress, needChunks, actualUse);
}

size_t CfsWritePage(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, const char *buffer,
                    bool sipSync, CFS_STORAGE_TYPE type)
{
    /* quick return if page is extend page */
    if (PageIsNew(buffer)) {
        return BLCKSZ;
    }
    ExtentLocation location = cfsLocationConverts[type](reln, forknum, logicBlockNumber, sipSync, EXTENT_OPEN_FILE);
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1,
                    (errmsg("could not write block %u in file \"%s\" headerNum: %u, this relation has been removed",
                            logicBlockNumber, FilePathName(location.fd), location.headerNum)));
            return 0;
        }
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsWritePage %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
    }
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    CfsExtentAddress *cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)location.extentOffset);

    /* get compress detail */
    RelFileCompressOption option;
    TransCompressOptions(reln->smgr_rnode.node, &option);
    uint16 chunkSize = cfsExtentHeader->chunk_size;

    /* compress page */
    uint8 nchunks;
    char *compressedBuffer = CfsCompressPage(buffer, &option, &nchunks);

    /* set address */
    uint8 need_chunks = option.compressPreallocChunks > nchunks ? (uint8)option.compressPreallocChunks : (uint8)nchunks;
    bool changed = ExtendChunksOfBlock(cfsExtentHeader, &location, need_chunks, nchunks);

    /* write chunks of compressed page */
    off_t extentStartOffset = location.extentStart * BLCKSZ;

    for (auto i = 0; i < nchunks; ++i) {
        auto buffer_pos = compressedBuffer + (long)chunkSize * i;
        off_t seekPos = OffsetOfPageCompressChunk(chunkSize, cfsExtentAddress->chunknos[i]);
        if (cfsExtentAddress->chunknos[i] > ((BLCKSZ / chunkSize) * CFS_LOGIC_BLOCKS_PER_EXTENT)) {
            *((uint32 *)NULL) = 1;
        }
        auto start = i;
        while (i < nchunks - 1 && cfsExtentAddress->chunknos[i + 1] == cfsExtentAddress->chunknos[i] + 1) {
            i++;
        }
        int write_amount = chunkSize * ((i - start) + 1);
        if ((seekPos + extentStartOffset) > (((BlockNumber)RELSEG_SIZE) * BLCKSZ)) {
            *((uint32 *)NULL) = 1;
        }
        int nbytes = FilePWrite(location.fd, buffer_pos, write_amount, seekPos + extentStartOffset,
                                (uint32)WAIT_EVENT_DATA_FILE_WRITE);
        if (nbytes != write_amount) {
            /* free compressed buffer */
            if (compressedBuffer != NULL && compressedBuffer != buffer) {
                pfree(compressedBuffer);
            }

            pca_buf_free_page(ctrl, location, false);
            if (nbytes < 0) {
                ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not write block %u in file \"%s\"",
                                                           logicBlockNumber,
                                                           FilePathName(location.fd))));
            }
            /* short write: complain appropriately */
            ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
                    errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes", logicBlockNumber,
                    FilePathName(location.fd), nbytes, BLCKSZ), errhint("Check free disk space.")));
        }
    }

    /* free compressed buffer */
    if (compressedBuffer != NULL && compressedBuffer != buffer) {
        pfree(compressedBuffer);
    }

    pca_buf_free_page(ctrl, location, changed);
    return BLCKSZ;
}

void InitExtentHeader(const ExtentLocation& location)
{
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NO_READ);
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    errno_t rc =  memset_s((char *)cfsExtentHeader, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");

    cfsExtentHeader->algorithm = location.algorithm;
    cfsExtentHeader->chunk_size = location.chrunk_size;

    pca_buf_free_page(ctrl, location, true);
}

void CfsExtendExtent(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, const char *buffer,
                     CFS_STORAGE_TYPE type)
{
    ExtentLocation location = cfsLocationConverts[type](reln, forknum, logicBlockNumber, true, EXTENT_OPEN_FILE);
    if (location.fd < 0) {
        return;
    }

    if (location.extentOffset == 0) {
        /* extend and fallocate */
        auto start = location.extentStart * BLCKSZ;
        InitExtentHeader(location);
        FileAllocate(location.fd, start, CFS_LOGIC_BLOCKS_PER_EXTENT * BLCKSZ);
    }
    (void)CfsWritePage(reln, forknum, logicBlockNumber, buffer, true, type);
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsExtendExtent %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
    }
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    if (pg_atomic_read_u32(&cfsExtentHeader->nblocks) < location.extentOffset + 1) {
        pg_atomic_write_u32(&cfsExtentHeader->nblocks, location.extentOffset + 1);
    }

    pca_buf_free_page(ctrl, location, true);
}

BlockNumber CfsNBlock(const RelFileNode &relFileNode, int fd, BlockNumber segNo, off_t len)
{
    RelFileCompressOption option;
    TransCompressOptions(relFileNode, &option);

    BlockNumber fileBlockNum = (BlockNumber) len / BLCKSZ;
    BlockNumber extentCount = fileBlockNum / CFS_EXTENT_SIZE;
    if (extentCount == 0) {
        /* to avoid half-write */
        return (BlockNumber)0;
    }
    BlockNumber result = (extentCount - 1) * (CFS_EXTENT_SIZE - 1);
    ExtentLocation location = {fd, relFileNode, (extentCount - 1) + segNo * CFS_EXTENT_COUNT_PER_FILE,
                               0, 0, extentCount * CFS_EXTENT_SIZE - 1,
                               (uint16)CHUNK_SIZE_LIST[option.compressChunkSize],
                               (uint8)option.compressAlgorithm};

    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsNBlock %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
    }
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    result = result + cfsExtentHeader->nblocks;
    Assert(result <= ((BlockNumber)RELSEG_SIZE));

    pca_buf_free_page(ctrl, location, false);
    return result;
}

void CfsMdPrefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync,
                   CFS_STORAGE_TYPE type)
{
    ExtentLocation location = cfsLocationConverts[type](reln, forknum, logicBlockNumber, skipSync, EXTENT_OPEN_FILE);
    if (location.fd < 0) {
        return;
    }
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsMdPrefetch %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
        return;
    }
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    auto cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)location.extentOffset);
    auto startOffset = location.extentStart * BLCKSZ;
    auto chunkSize = cfsExtentHeader->chunk_size;

    for (uint8 i = 0; i < cfsExtentAddress->nchunks; i++) {
        off_t seekPos = startOffset + OffsetOfPageCompressChunk(chunkSize, cfsExtentAddress->chunknos[i]);
        int range = 1;
        while (i < cfsExtentAddress->nchunks - 1 &&
               cfsExtentAddress->chunknos[i + 1] == cfsExtentAddress->chunknos[i] + 1) {
            i++;
            range++;
        }
        (void)FilePrefetch(location.fd, seekPos, chunkSize * range, (uint32)WAIT_EVENT_DATA_FILE_PREFETCH);
    }
    pca_buf_free_page(ctrl, location, false);
}

off_t CfsMdTruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync,
                    CFS_STORAGE_TYPE type)
{
    ExtentLocation location = cfsLocationConverts[type](reln, forknum, logicBlockNumber, skipSync, EXTENT_OPEN_FILE);
    /* if logicBlockNumber is the first block of extent, truncate all after blocks */
    if (logicBlockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT == 0) {
        return location.extentStart * BLCKSZ;  // ok
    }

    auto truncateOffset = (location.headerNum + 1) * BLCKSZ;
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsMdTruncate %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
    }
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    for (int i = (int)location.extentOffset; i < CFS_LOGIC_BLOCKS_PER_EXTENT; i++) {
        auto cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)i);

        cfsExtentAddress->nchunks = 0;
        cfsExtentAddress->checksum = AddrChecksum32(cfsExtentAddress, cfsExtentAddress->allocated_chunks);
    }

    uint16 max = 0;
    for (uint16 i = 0; i < location.extentOffset; i++) {
        auto cfsExtentAddress = GetExtentAddress(cfsExtentHeader, i);
        for (int j = 0; j < cfsExtentAddress->allocated_chunks; j++) {
            max = (max > cfsExtentAddress->chunknos[j]) ? max : cfsExtentAddress->chunknos[j];
        }
    }

    pg_atomic_write_u32(&cfsExtentHeader->nblocks, location.extentOffset);

    /* need sync cfs header */
    pca_buf_free_page(ctrl, location, true);

    /* File allocate (file hole) */
    uint32 start = location.extentStart * BLCKSZ + max * cfsExtentHeader->chunk_size;
    uint32 len = (uint32)((CFS_MAX_LOGIC_CHRUNKS_NUMBER(cfsExtentHeader->chunk_size) - max) *
                          cfsExtentHeader->chunk_size);
    if (len >= MIN_FALLOCATE_SIZE) {
        start += (len % MIN_FALLOCATE_SIZE);
        len -= (len % MIN_FALLOCATE_SIZE);
        FileAllocate(location.fd, start, len);
    }
    return truncateOffset;
}

/*******************************************************for file / page repair*************************************/
int CfsGetPhysicsFD(const RelFileNode &relnode, BlockNumber logicBlockNumber)
{
    errno_t rc;
    char *firstpath = relpathperm(relnode, MAIN_FORKNUM);
    char path[MAXPGPATH] = {0};

    uint32 segno = logicBlockNumber / CFS_LOGIC_BLOCKS_PER_FILE;

    if (segno == 0) {
        rc = sprintf_s(path, MAXPGPATH, "%s%s", firstpath, COMPRESS_STR);
    } else {
        rc = sprintf_s(path, MAXPGPATH, "%s.%u%s", firstpath, segno, COMPRESS_STR);
    }
    securec_check_ss(rc, "", "");

    int fd = BasicOpenFile((char*)path, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(WARNING, (errcode_for_file_access(),
            errmsg("[file repair] could not open file \"%s\": logicBlockNumber:%u, segno:%u",
                   path, logicBlockNumber, segno)));
        pfree(firstpath);
        return -1;
    }

    pfree(firstpath);
    return fd;
}

void CfsHeaderPageCheckAndRepair(SMgrRelation reln, BlockNumber logicBlockNumber,
    char *pca_page_res, uint32 strLen, bool *need_repair_pca)
{
    errno_t rc = 0;
    ExtentLocation location =
        cfsLocationConverts[COMMON_STORAGE](reln, MAIN_FORKNUM, logicBlockNumber, true, EXTENT_OPEN_FILE);

    /* load the pca page directly */
    CfsExtentHeader *pca_disk = (CfsExtentHeader *)palloc0(BLCKSZ);
    CfsExtentHeader *pca_mem = (CfsExtentHeader *)palloc0(BLCKSZ);
    int nbytes = FilePRead(location.fd, (char *)pca_disk, BLCKSZ, location.headerNum * BLCKSZ,
                           (uint32)WAIT_EVENT_DATA_FILE_READ);
    if (nbytes != BLCKSZ) {
        pfree(pca_disk);
        pfree(pca_mem);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsHeaderPageCheckAndRepair %s", FilePathName(location.fd))));
    }

    /* lock the extent by LW_EXCLUSIVE */
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_EXCLUSIVE, PCA_BUF_NO_READ);

    CfsHeaderPagerCheckStatus disk_status = CheckAndRepairCompressAddress(pca_disk, location.chrunk_size,
                                                                          location.algorithm, location);
    CfsHeaderPagerCheckStatus mem_status = CFS_HEADER_CHECK_STATUS_ERROR;
    if (ctrl->load_status == CTRL_PAGE_IS_LOADED) {
        rc = memcpy_s(pca_mem, BLCKSZ, ctrl->pca_page, BLCKSZ);
        securec_check(rc, "\0", "\0");
        mem_status = CheckAndRepairCompressAddress(pca_mem, location.chrunk_size,
                                                   location.algorithm, location);
    }
    pfree(pca_disk);
    pfree(pca_mem);

    /* get pca page info */
    if (pca_page_res != NULL) {
        if (ctrl->load_status != CTRL_PAGE_IS_NO_LOAD) {
            rc = snprintf_s(pca_page_res, strLen, strLen - 1,
                            "Relative pca page status: [Mem: \"%s\", Disk: \"%s\"], so %s.",
                            CfsHeaderPagerCheckStatusList[mem_status],
                            CfsHeaderPagerCheckStatusList[disk_status],
                            (disk_status != CFS_HEADER_CHECK_STATUS_OK && mem_status != CFS_HEADER_CHECK_STATUS_OK) ?
                                "need to repair the whole extent" : "the pca page verified ok.");
        } else {
            rc = snprintf_s(pca_page_res, strLen, strLen - 1,
                            "Relative pca page status: [Mem: \"not in memory\", Disk: \"%s\"], so %s.",
                            CfsHeaderPagerCheckStatusList[disk_status],
                            (disk_status != CFS_HEADER_CHECK_STATUS_OK && mem_status != CFS_HEADER_CHECK_STATUS_OK) ?
                                "need to repair the whole extent" : "the pca page verified ok.");
        }
        securec_check_ss(rc, "\0", "\0");
    }

    /* much strickly */
    *need_repair_pca = false;
    if (disk_status != CFS_HEADER_CHECK_STATUS_OK && mem_status == CFS_HEADER_CHECK_STATUS_OK) {
        /* flush the origin correct page from memory into disk */
        pca_buf_free_page(ctrl, location, true);
        return ;
    } else if (disk_status == CFS_HEADER_CHECK_STATUS_OK && mem_status != CFS_HEADER_CHECK_STATUS_OK) {
        /* the next time the other thread will load it from disk into memory */
        PCA_SET_NO_READ(ctrl);
    } else if (disk_status != CFS_HEADER_CHECK_STATUS_OK && mem_status != CFS_HEADER_CHECK_STATUS_OK) {
        /* Neither of them(mem and disk) is ok, need to repair the whole extent */
        *need_repair_pca = true;
    }

    pca_buf_free_page(ctrl, location, false);
}

/* only memory operation */
void WriteRepairFile_Compress_Block(RelFileCompressOption option, char *buf, char *compressed_buf,
    CfsExtentAddress *cfsExtentAddress, CfsExtentHeader *cfsExtentHeader)
{
    errno_t rc = 0;
    uint16 chunkSize = cfsExtentHeader->chunk_size;

    /* compress page */
    uint8 nchunks;
    char *compressedBuffer = CfsCompressPage(buf, &option, &nchunks);

    /* set address */
    uint8 need_chunks = option.compressPreallocChunks > nchunks ? (uint8)option.compressPreallocChunks : (uint8)nchunks;
    (void)ExtendChunksOfBlockCore(cfsExtentHeader, cfsExtentAddress, need_chunks, nchunks);

    /* copy compressed data into dst */
    char *dst = compressed_buf + (long)(cfsExtentAddress->chunknos[0] - 1) * chunkSize;
    rc = memcpy_s(dst, need_chunks * chunkSize, compressedBuffer, need_chunks * chunkSize);
    securec_check(rc, "\0", "\0");

    /* free compressed buffer */
    if (compressedBuffer != NULL && compressedBuffer != buf) {
        pfree(compressedBuffer);
    }

    return ;
}

int WriteRepairFile_Compress_Extent_Impl(const RelFileCompressOption &option, int fd, char* reapirpath, char *buf,
                                         BlockNumber offset, uint32 blocks)
{
    errno_t rc = 0;
    if (lseek(fd, offset * BLCKSZ, SEEK_SET) < 0) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("[file repair] could not seek reapir file %s",
            reapirpath)));
        return -1;
    }

    /* alloc a extent, for write only 1 times. */
    char *compressed_buf = (char*)palloc0(CFS_EXTENT_SIZE * BLCKSZ);
    rc = memset_s(compressed_buf, CFS_EXTENT_SIZE * BLCKSZ, 0, CFS_EXTENT_SIZE * BLCKSZ);
    securec_check(rc, "\0", "\0");

    /* init the header first */
    CfsExtentHeader *cfsExtentHeader = (CfsExtentHeader *)(void *)(compressed_buf +
                                                                   CFS_LOGIC_BLOCKS_PER_EXTENT * BLCKSZ);
    cfsExtentHeader->chunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize];
    cfsExtentHeader->algorithm = option.compressAlgorithm;

    /* then fill all the chrunks */
    for (uint32 blkno = 0; blkno < blocks; blkno++) {
        auto cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)blkno);
        WriteRepairFile_Compress_Block(option,
                                       buf + (long)blkno * BLCKSZ,
                                       compressed_buf,
                                       cfsExtentAddress, cfsExtentHeader);
        /* update the header */
        (void)pg_atomic_fetch_add_u32(&cfsExtentHeader->nblocks, 1);
    }
    cfsExtentHeader->recycleInOrder = 1;

    /* get max chrunk number */
    uint32 max_chrunkno = cfsExtentHeader->allocated_chunks;

    /* write compressed buf into disk */
    if (write(fd, compressed_buf, CFS_EXTENT_SIZE * BLCKSZ) != (CFS_EXTENT_SIZE * BLCKSZ)) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(WARNING, (errcode_for_file_access(), errmsg("[file repair] could not write to temp file %s",
            reapirpath)));
        pfree(compressed_buf);
        return -1;
    }

    /* File allocate (file hole) */
    uint32 start = offset * BLCKSZ + max_chrunkno * cfsExtentHeader->chunk_size;
    uint32 len =
        (CFS_MAX_LOGIC_CHRUNKS_NUMBER(cfsExtentHeader->chunk_size) - max_chrunkno) * cfsExtentHeader->chunk_size;
    if (len >= MIN_FALLOCATE_SIZE) {
        len -= start % MIN_FALLOCATE_SIZE;
        start += start % MIN_FALLOCATE_SIZE;
        FileAllocateDirectly(fd, reapirpath, start, len);
    }

    if (fsync(fd) != 0) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("[file repair] could not fsync temp file %s",
            reapirpath)));
        pfree(compressed_buf);
        return -1;
    }

    pfree(compressed_buf);
    return 0;
}

int WriteRepairFile_Compress_extent(SMgrRelation reln, BlockNumber logicBlockNumber,
    char* reapirpath, char *buf, BlockNumber offset, uint32 blocks)
{
    /* get Compress Option from opt */
    RelFileCompressOption option;
    TransCompressOptions(reln->smgr_rnode.node, &option);

    /* get physics file fd, get real fd from relnode and logicBlockNumber */
    int fd = CfsGetPhysicsFD(reln->smgr_rnode.node, logicBlockNumber);
    if (fd < 0) {
        return fd;
    }

    ExtentLocation location =
        cfsLocationConverts[COMMON_STORAGE](reln, MAIN_FORKNUM, logicBlockNumber, true, EXTENT_OPEN_FILE);

    /* lock the extent by LW_EXCLUSIVE */
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_EXCLUSIVE, PCA_BUF_NO_READ);

    int rc = WriteRepairFile_Compress_Extent_Impl(option, fd, reapirpath, buf, offset, blocks);

    (void)close(fd);
    PCA_SET_NO_READ(ctrl);
    pca_buf_free_page(ctrl, location, false);
    return rc;
}

const int REPAIR_LEN = 8;
int WriteRepairFile_Compress(const RelFileNode &rd_node, int fd, char* path, char *buf,
                             BlockNumber blkno_segno_offset, uint32 blk_cnt)
{
    errno_t rc = 0;
    char *reapirpath = (char *)palloc(strlen(path) + REPAIR_LEN);
    rc = sprintf_s(reapirpath, strlen(path) + REPAIR_LEN, "%s.repair", path);
    securec_check_ss(rc, "", "");

    size_t off = (blkno_segno_offset / CFS_LOGIC_BLOCKS_PER_EXTENT) * CFS_EXTENT_SIZE;

    RelFileCompressOption option;
    TransCompressOptions(rd_node, &option);

    uint32 extent_count =
        (blk_cnt / CFS_LOGIC_BLOCKS_PER_EXTENT) + ((blk_cnt % CFS_LOGIC_BLOCKS_PER_EXTENT) != 0 ? 1 : 0);
    for (uint32 i = 0; i < extent_count; i++) {
        uint32 blocks = CFS_LOGIC_BLOCKS_PER_EXTENT;
        if ((i == (extent_count - 1)) && ((blk_cnt % CFS_LOGIC_BLOCKS_PER_EXTENT) != 0)) {
            blocks = (blk_cnt % CFS_LOGIC_BLOCKS_PER_EXTENT);
        }

        /* write every compression extent */
        rc = WriteRepairFile_Compress_Extent_Impl(option, fd, reapirpath,
                                                  buf + (long)i * CFS_LOGIC_BLOCKS_PER_EXTENT * BLCKSZ,
                                                  (uint32)(off + i * CFS_EXTENT_SIZE), blocks);
        if (rc != 0) {
            pfree(reapirpath);
            return rc;
        }
    }

    pfree(reapirpath);
    return 0;
}

/*******************************************xlog recored and redo***************************************************/
void CfsShrinkRecord(const RelFileNode &node, ForkNumber forknum)
{
    CfsShrink_t data;
    START_CRIT_SECTION();

    data.node = node;
    data.forknum = forknum;

    XLogBeginInsert();
    XLogRegisterData((char *)&data, sizeof(CfsShrink_t));
    XLogRecPtr lsn = XLogInsert((RmgrId)RM_COMPRESSION_REL_ID, XLOG_CFS_SHRINK_OPERATION);
    END_CRIT_SECTION();

    XLogWaitFlush(lsn);
}

void CfsShrinkRedo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_CFS_SHRINK_OPERATION) {
        /* XLOG_CFS_SHRINK_OPERATION is preformed with nowait = true in standby
            record xlog XLOG_CFS_SHRINK_OPERATION after th operation is done in master,
            so CfsShrinkRedo is just applied for standby.
        */
        CfsShrink_t *data = (CfsShrink_t *)(void *)XLogRecGetData(record);
        CfsShrinkerShmemListPush(data->node, data->forknum, data->parttype);
    } else {
        ereport(PANIC, (errmsg("CfsShrink_redo: unknown op code %hu", info)));
    }
}

const char* CfsShrinkTypeName(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_CFS_SHRINK_OPERATION) {
        return "start_cfs_shrink";
    } else {
        return "unkown_type";
    }
}

/* fd is not vfd, it is real fd */
void CfsReadFile(int fd, void *buf, int size, int64 offset)
{
    int32 curr_size;
    int32 total_size = 0;
    do {
        curr_size = (int32)pread64(fd, ((char *)buf + total_size), (uint32)(size - total_size), offset);
        if (curr_size == -1) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Cfs read file error")));
        }

        total_size += curr_size;
        offset += curr_size;
    } while (curr_size > 0);

    if (total_size != size) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                        errmsg(" Cfs read file size mismatch: expected %d, read %d", size, total_size)));
    }
}

/* fd is not vfd, it is real fd */
void CfsWriteFile(int fd, const void *buf, int size, int64 offset)
{
    int write_size = 0;
    uint32 try_times = 0;

    while (try_times < CFS_WRITE_RETRY_TIMES) {
        write_size = (int)pwrite64(fd, buf, (unsigned long)size, offset);
        if (write_size == 0) {
            try_times++;
            pg_usleep(1000L);
            continue;
        } else if (write_size < 0) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("Cfs write file error")));
        } else {
            break;
        }
    }
    if (write_size != size) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("Cfs Write file mismatch: expected %d, written %d", size, write_size)));
    }
}

static int CfsCreateFile(const char* file_name)
{
    int fd = -1;
    fd = open(file_name, (O_RDWR | O_SYNC | O_DIRECT | PG_BINARY | O_CREAT), S_IRUSR | S_IWUSR);
    if (fd == -1) {
        ereport(PANIC, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("Could not create file %s",
            file_name)));
    }

    return fd;
}

void CfsRemoveFile(const char* file_name)
{
    struct stat st;

    Assert(file_name != NULL);

    if (stat(file_name, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("dest is a directory: \"%s\"", file_name)));
        }
        if (unlink(file_name) != 0) {
            ereport(PANIC, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("Could not remove the file: %s",
                file_name)));
        }
    } else if (!(errno == ENOENT || errno == ENOTDIR || errno == EACCES)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not access file \"%s\"", file_name)));
    }
}


/*********************************deal with page in dw**********************************************************/
/* in recovery, pca need ro be corrected when it is used at fisrt time in double write */
void MdRecoveryPcaPage(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, bool skipFsync)
{
    /* it must be compressed file at now */
    Assert(reln != NULL && IS_COMPRESSED_MAINFORK(reln, forknum));

    pca_page_ctrl_t *ctrl = NULL;
    CfsExtentHeader *extHeader = NULL;
    CfsExtentAddress *extAddr = NULL;

    /* buffer init is ahead of dw, buffer can be used for reading pca */
    ExtentLocation location = StorageConvert(reln, forknum, blocknum, skipFsync, WRITE_BACK_OPEN_FILE);
    // protect ext from reading and writing during pca recovery
    ctrl = pca_buf_read_page(location, LW_EXCLUSIVE, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed in MdRecoveryPcaPage %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
    }

    extHeader = ctrl->pca_page;
    /* traverse pca_page to correct allocated_chunks */
    uint32 maxChkId = 0;
    for (uint32 blkno = 0; blkno < extHeader->nblocks; blkno++) {
        extAddr = GetExtentAddress(extHeader, (uint16)blkno);
        for (uint32 chunkNum = 0; chunkNum < extAddr->allocated_chunks; chunkNum++) {
            /* make sure that each chunknos is valid */
            Assert((extAddr->chunknos[chunkNum] != 0) &&
                (extAddr->chunknos[chunkNum] <= (BLCKSZ / extHeader->chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT)));
            maxChkId = (extAddr->chunknos[chunkNum] > maxChkId) ? extAddr->chunknos[chunkNum] : maxChkId;
        }
    }

    extHeader->allocated_chunks = maxChkId;
    pca_buf_free_page(ctrl, location, true);
}

/********************************deal with assist file *************************************************************/

/* in recovery, assist file should be dealt with in dw if assist files exist */
void MdAssistFileProcess(SMgrRelation relation, const char *assistInfo, int assistFd)
{
    Assert(assistInfo != NULL && assistFd != -1);

    int nbytes = 0;
    pca_page_ctrl_t *ctrl = NULL;
    CfsExtentHeader *extHeader = NULL;
    CfsExtInfo *extInfo = (CfsExtInfo *)(void *)assistInfo;

    /* it must be compressed file at now */
    Assert(relation != NULL && IS_COMPRESSED_MAINFORK(relation, extInfo->forknum));

    ExtentLocation location = StorageConvert(relation, extInfo->forknum,
                                             extInfo->extentNumber * CFS_LOGIC_BLOCKS_PER_EXTENT,
                                             false, WRITE_BACK_OPEN_FILE);
    ctrl = pca_buf_read_page(location, LW_EXCLUSIVE, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to MdAssistFileProcess %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
    }

    extHeader = ctrl->pca_page;
    if (extHeader->recycleInOrder) {
        pca_buf_free_page(ctrl, location, false);
        return;
    }

    char *unaligned_buf = (char *) palloc(CFS_EXTENT_SIZE * BLCKSZ + BLCKSZ);
    char *aligned_buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    /* assistfd do not use smgropen, so it is not vfd */
    CfsReadFile(assistFd, aligned_buf, CFS_EXTENT_SIZE * BLCKSZ, 0);

    /* if FileWrite can be used instand of FilePWrite */
    nbytes = FilePWrite(location.fd, aligned_buf, CFS_EXTENT_SIZE * BLCKSZ,
                        location.extentStart * BLCKSZ, (uint32)WAIT_EVENT_DATA_FILE_WRITE);
    if (nbytes != CFS_EXTENT_SIZE * BLCKSZ) {
        pfree(unaligned_buf);
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode_for_file_access(), errmsg("Failed to write the %d ext to file %s",
            (int)extInfo->extentNumber, FilePathName(location.fd))));
    }

    /* pca is changed, so pca buffer need to be reloaded in next use */
    pfree(unaligned_buf);
    PCA_SET_NO_READ(ctrl);
    pca_buf_free_page(ctrl, location, false);
}

/******************************************chunk recycle**************************************************/
ExtentLocation FormExtLocation(SMgrRelation sRel, BlockNumber logicBlockNumber)
{
    RelFileCompressOption option;
    TransCompressOptions(sRel->smgr_rnode.node, &option);

    BlockNumber extentNumber = logicBlockNumber / CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentOffset = logicBlockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentStart = (extentNumber * CFS_EXTENT_SIZE) % CFS_MAX_BLOCK_PER_FILE; // 0   129      129*2     129*3
    BlockNumber extentHeader = extentStart + CFS_LOGIC_BLOCKS_PER_EXTENT;  //              128 129+128  129*2+128

    return {
        .fd = -1,
        .relFileNode = sRel->smgr_rnode.node,
        .extentNumber = extentNumber,
        .extentStart = extentStart,
        .extentOffset = extentOffset,
        .headerNum = extentHeader,
        .chrunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize],
        .algorithm = (uint8)option.compressAlgorithm
    };
}

void CfsSortOutChunk(const ExtentLocation &location, CfsExtentHeader *srcExtPca, char *alignbuf)
{
    char *srcBuf = alignbuf; // src buf includes 128 pages
    char *assistBuf = alignbuf + CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ; // assist includes 129 page;
    CfsExtentAddress *srcExtAddr = NULL;
    CfsExtentAddress *assistExtAddr = NULL;

    int nbytes = FilePRead(location.fd, srcBuf, CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ,
        location.extentStart * BLCKSZ, (uint32)WAIT_EVENT_DATA_FILE_WRITE);
    if (nbytes != CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("Failed to write the %d ext to file %s",
            (int)location.extentStart, FilePathName(location.fd))));
    }

    CfsExtentHeader *assistPca = (CfsExtentHeader *)(void *)(assistBuf + CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ);
    errno_t rc = memcpy_s(assistPca, offsetof(CfsExtentHeader, cfsExtentAddress),
        srcExtPca, offsetof(CfsExtentHeader, cfsExtentAddress));
    securec_check(rc, "\0", "\0");

    uint16 chunknum = 0;  // chunkid start from 1, 0 means invalid chunkid
    for (BlockNumber i = 0; i < srcExtPca->nblocks; i++) {
        srcExtAddr = GetExtentAddress(srcExtPca, (uint16)i);
        assistExtAddr = GetExtentAddress(assistPca, (uint16)i);

        Assert(srcExtAddr->nchunks <= BLCKSZ / srcExtPca->chunk_size);

        assistExtAddr->allocated_chunks = srcExtAddr->nchunks;
        assistExtAddr->nchunks = srcExtAddr->nchunks;

        for (uint32 j = 0; j < srcExtAddr->nchunks; j++) {
            chunknum++;
            assistExtAddr->chunknos[j] = chunknum;
            Assert(srcExtAddr->chunknos[j] <= (BLCKSZ / srcExtPca->chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));

            /* copy seperate chunk together */
            rc = memcpy_s((void*)(assistBuf + (long)(assistExtAddr->chunknos[j] - 1) * srcExtPca->chunk_size),
                          srcExtPca->chunk_size,
                (void *)(srcBuf + (long)(srcExtAddr->chunknos[j] - 1) * srcExtPca->chunk_size), srcExtPca->chunk_size);
            securec_check_ss(rc, "\0", "\0");
        }

        /* checksum need to culculate at the end */
        assistExtAddr->checksum = AddrChecksum32(assistExtAddr, assistExtAddr->allocated_chunks);
    }

    assistPca->allocated_chunks = chunknum;
    assistPca->recycleInOrder = 1; // set flag, showing the ext is in order.
}

void CfsPunchHole(const ExtentLocation &location, CfsExtentHeader *assistPca)
{
    uint32 chunksum = assistPca->allocated_chunks;

    uint32 punchOffset = chunksum * assistPca->chunk_size;
    punchOffset = TYPEALIGN(MIN_FALLOCATE_SIZE, punchOffset); // alignment of 4K, the uint of punch hole

    uint32 start = location.extentStart * BLCKSZ + punchOffset;
    uint32 punchSize = (location.headerNum - location.extentStart) * BLCKSZ - punchOffset;

    if (punchSize != 0) {
        FileAllocate(location.fd, start, punchSize);
    }
}

/* recycle chunk in ext */
void CfsRecycleChunkInExt(ExtentLocation location, int assistfd, char *alignbuf)
{
    char *srcBuf = alignbuf; // src buf includes 128 pages
    char *assistBuf = srcBuf + CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ; // assist includes 129 page;
    char *assistInfobuf = assistBuf + CFS_EXTENT_SIZE * BLCKSZ; // assist info is stored in last added page

    CfsExtInfo *extInfo = (CfsExtInfo *)(void *)assistInfobuf;
    location.extentNumber = extInfo->extentNumber;
    location.extentStart = (extInfo->extentNumber % CFS_EXTENT_COUNT_PER_FILE) * CFS_EXTENT_SIZE;
    location.headerNum = location.extentStart + CFS_LOGIC_BLOCKS_PER_EXTENT;

    /* lock pca */
    // protect ext from reading and writing
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_EXCLUSIVE, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsRecycleChunk %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
    }
    CfsExtentHeader *srcExtPca = ctrl->pca_page;
    if (srcExtPca->recycleInOrder) {
        pca_buf_free_page(ctrl, location, false);
        return;
    }

    /* make chunk in continuous field in order */
    errno_t rc = memset_s(assistBuf, CFS_EXTENT_SIZE * BLCKSZ, 0, CFS_EXTENT_SIZE * BLCKSZ);
    securec_check(rc, "\0", "\0");
    CfsSortOutChunk(location, srcExtPca, alignbuf);

    extInfo->assistFlag = 1;
    CfsWriteFile(assistfd, assistBuf, CFS_EXTENT_SIZE * BLCKSZ + BLCKSZ, 0);

    Assert(ctrl->ref_num == 1);
    int nbytes = FilePWrite(location.fd, assistBuf, CFS_EXTENT_SIZE * BLCKSZ, location.extentStart * BLCKSZ,
                            (uint32)WAIT_EVENT_DATA_FILE_WRITE);
    if (nbytes != CFS_EXTENT_SIZE * BLCKSZ) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode_for_file_access(), errmsg("Failed to write the %d ext to file %s",
            (int)extInfo->extentNumber, FilePathName(location.fd))));
    }

    extInfo->assistFlag = 0;
    CfsWriteFile(assistfd, assistBuf, BLCKSZ, CFS_EXTENT_SIZE * BLCKSZ);

    // use fallocate to punch holes at the tail of the pcd in ext
    CfsExtentHeader *assistPca = (CfsExtentHeader *)(void *)(assistBuf + CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ);
    CfsPunchHole(location, assistPca);

    /* free pca buffer */
    PCA_SET_NO_READ(ctrl);
    pca_buf_free_page(ctrl, location, false);
}

/**********************************************************************************************************

 aligned_buf:               |         128 page          |           129 page            |     1 page
                  srcbuf start point          assist buf start point           assist info start point
                                              ________________________________________________________
                                                             assist file content (130 page)

 **********************************************************************************************************/
void CfsRecycleChunkProc(SMgrRelation reln, ForkNumber forknum)
{
    ExtentLocation location = FormExtLocation(reln, 0);
    MdfdVec *mdfd = CfsMdOpenReln(reln, forknum, EXTENSION_RETURN_NULL);
    if (unlikely(mdfd == NULL)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("can not get vfd using CfsMdOpen")));
    }

    /* check if it is compressed file */
    if (!IS_COMPRESSED_MAINFORK(reln, forknum)) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("it is not compressed file \"%s\"", FilePathName(mdfd->mdfd_vfd))));
    }

    char filePath[MAXPGPATH];
    errno_t rc = snprintf_s(filePath, MAXPGPATH, MAXPGPATH - 1,
                            "global/pg_dw_ext_chunk/%u_assist_tmp", reln->smgr_rnode.node.relNode);
    securec_check_ss(rc, "\0", "\0");

    int assistfd = CfsCreateFile(filePath);

    // alloc buffer for pca and assist pca
    char *unaligned_buf = (char *) palloc(CFS_PCA_AND_ASSIST_BUFFER_SIZE + BLCKSZ);
    char *aligned_buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    // assist info is stored in last added page
    char *assistInfobuf = aligned_buf + CFS_PCA_AND_ASSIST_BUFFER_SIZE - BLCKSZ;

    rc = memset_s(aligned_buf, CFS_PCA_AND_ASSIST_BUFFER_SIZE, 0, CFS_PCA_AND_ASSIST_BUFFER_SIZE);
    securec_check(rc, "\0", "\0");

    CfsExtInfo *extInfo = (CfsExtInfo *)(void *)assistInfobuf;
    extInfo->forknum = forknum;
    extInfo->rnode = reln->smgr_rnode.node;

    BlockNumber extIdx = 0;
    while (mdfd != NULL) {
        BlockNumber nBlocks = CfsGetBlocks(reln, forknum, mdfd);
        if (nBlocks == 0) {
            mdfd = mdfd->mdfd_chain;
            continue;
        }

        location.fd = mdfd->mdfd_vfd;
        for (uint32 i = 0; i * CFS_LOGIC_BLOCKS_PER_EXTENT < nBlocks; i++) {
            extInfo->extentNumber = extIdx;
            CfsRecycleChunkInExt(location, assistfd, aligned_buf);
            extIdx++;
        }

        mdfd = mdfd->mdfd_chain;
    }

    pfree(unaligned_buf);
    (void)close(assistfd);
    CfsRemoveFile(filePath);
}


void CfsRecycleChunk(SMgrRelation reln, ForkNumber forknum)
{
    /* it must be compressed file at now. */
    if (reln == NULL || !IS_COMPRESSED_MAINFORK(reln, forknum)) {
        return;
    }

    /* return until the massion is done */
    CfsRecycleChunkProc(reln, forknum);
}
