/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
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
#include "storage/smgr/cfs_addressing.h"

#define CFS_PCA_AND_ASSIST_BUFFER_SIZE (2 * CFS_EXTENT_SIZE * BLCKSZ)
#define CFS_WRITE_RETRY_TIMES 8

int CfsGetFd(SMgrRelation sRel, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync, int type);

/** extend chunks for blocks
 @param[in/out] cfsExtentHeader     pca page header.
 @param[in/out] location            extent loacation information.
 @param[in]     needChunks          the number of chunks needed for a block.
                                    (chunk can be preallocated for a compressPreallocChunks number).
 @param[in]     actualUse           the number chunks actual used for a block.
 @param[in/out] freeChunkLock       the lock that control allocated_chunk_usages bitmap.
 @return return bool indicate that the extentheader is changed or not. */
static bool ExtendChunksOfBlock(CfsExtentHeader *cfsExtentHeader, ExtentLocation *location, uint8 needChunks,
                                uint8 actualUse, LWLock *freeChunkLock);

/** return bool indicate that the extentheader is changed.
 @param[in/out] cfsExtentHeader     pca page header.
 @param[in/out] cfsExtentAddress    chunk information for a block.
 @param[in]     needChunks          the number of chunks needed for a block.
                                    (chunk can be preallocated for a compressPreallocChunks number).
 @param[in]     actualUse           the number chunks actual used for a block.
 @param[in/out] freeChunkLock       the lock that control allocated_chunk_usages bitmap.
 @return return bool indicate that the extentheader is changed or not. */
static bool ExtendChunksOfBlockCore(CfsExtentHeader *cfsExtentHeader, CfsExtentAddress *cfsExtentAddress,
                                    uint8 needChunks, uint8 actualUse, LWLock *freeChunkLock);

/** write file for compress
 @param[in]     fd        file dsecriptor.
 @param[in/out] buf       page buffer.
 @param[in]     size      the size of file that need to be writen.
 @param[in]     offset    offset of the data that start to wirte. */
static void CfsWriteFile(int fd, const void *buf, int size, int64 offset);

/** write compress block for repair file
 @param[in]     option            compress information.
 @param[in/out] buf               page buffer.
 @param[in/out] compressed_buf    compressed page buffer.
 @param[in/out] cfsExtentAddress  chunk information for a block.
 @param[in/out] cfsExtentHeader   pca page header. */
static void WriteRepairFile_Compress_Block(RelFileCompressOption option, char *buf, char *compressed_buf,
                                           CfsExtentAddress *cfsExtentAddress, CfsExtentHeader *cfsExtentHeader);

/** prunch hole for the extent
 @param[in/out] location      extent loacation information.
 @param[in/out] assistPca     pca page header. */
static void CfsPunchHole(const ExtentLocation &location, CfsExtentHeader *assistPca);

/** try to recyle one extent, make it alosely arranged in order.
 @param[in]     reln           relation information.
 @param[in]     forknum        file type for relation.
 @param[in/out] oldLocation    extent loacation information. */
static void CfsRecycleOneExtent(SMgrRelation reln, ForkNumber forknum, ExtentLocation *oldLocation);

/** recycle chunk in all extents
 @param[in/out] location      extent loacation information.
 @param[in]     assistfd      assist file dsecriptor.
 @param[in/out] alignbuf      src page buffer.
 @param[in/out] ctrl          page ctrl to store pca page. */
static void CfsRecycleChunkInExt(ExtentLocation location, int assistfd, char *alignbuf, pca_page_ctrl_t *ctrl);

/** sort out chunk in order
 @param[in/out] location      extent loacation information.
 @param[in/out] srcExtPca     pca page header.
 @param[in/out] alignbuf      src page buffer. */
static void CfsSortOutChunk(const ExtentLocation &location, CfsExtentHeader *srcExtPca, char *alignbuf);

char* CfsHeaderPagerCheckStatusList[] = {"verified success.",
    "verified failed but repaired some values.",
    "verified failed."};

void CfsRecycleChunk(SMgrRelation reln, ForkNumber forknum);
void CfsShrinkerShmemListPush(const RelFileNode &rnode, ForkNumber forknum, char parttype);

int CfsReadPage(SMgrRelation reln, const RelFileNode &relNode, int fd, int extent_size, ForkNumber forknum,
                BlockNumber logicBlockNumber, char *buffer, EXTEND_STORAGE_TYPE type)
{
    errno_t rc;
    int nbytes;
    ExtentLocation location =
        g_location_convert[type](reln, relNode, fd, extent_size, forknum, logicBlockNumber);
    if (location.fd < 0) {
        return -1;
    }
    /* if it's segment page and we don't compress the page, write the original page. Cases
    that we do this are:
        - It's in 1th or 2th segment logical file that we don't compress blocks by design.
        - It's not data block.
        - It's under a DSS storage.
        - The cfs extent of the block is cross two 1G physical file, punch hole is meaningless. */
    if (location.is_segment_page && (!location.is_compress_allowed)) {
        nbytes = DirectFilePRead(location.fd, buffer, BLCKSZ, location.GetBlockPhysicalOffset(),
                                 (uint32)WAIT_EVENT_DATA_FILE_WRITE);
        ereport(DEBUG2, (errmodule(MOD_SEGMENT_PAGE),
                      errmsg("don't compress block due to it's a slice-acrossed block,"
                             "logicBlockNumber:%u, fd:%d, extent_size:%d, forknum:%d,type:%d,"
                             "RelFileNode.relNode:%d, RelFileNode.opt:%d",
                             logicBlockNumber, fd, extent_size, forknum, type, relNode.relNode, relNode.opt)));
        return nbytes;
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
    TransCompressOptions(relNode, &option);

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
        if (location.is_segment_page) {
            nbytes = DirectFilePRead(location.fd, bufferPos, readAmount, seekPos,
                                     (uint32)WAIT_EVENT_DATA_FILE_READ);
        } else {
            nbytes = FilePRead(location.fd, bufferPos, readAmount, seekPos, (uint32)WAIT_EVENT_DATA_FILE_READ);
        }
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

/** flush a single compress block into the compressed disk file.
 @param[in]     reln        the SMgrRelation for the block.
 @param[in]     forknum     the forknum of the relation.
 @param[in]     location    the extent location of the block need to be flushed.
 @param[in]     blocknum    the block number.
 @return true if we succeed to flush the compressed page, otherwise return false. */
static bool CfsFlushBlock(SMgrRelation reln, ForkNumber forknum,
                          const ExtentLocation &location, BlockNumber blocknum)
{
    pc_chunk_number_t seekPosChunk;
    pc_chunk_number_t lastChunk;
    bool firstEnter = true;
    uint16_t chunkSize =  location.chunk_size;
    off_t seekPos;
    bool pcaNeedFlush = false;

    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("could not write back %u in file \"%s\" headerNum: %u, relation has been removed",
                     blocknum, FilePathName(location.fd), location.headerNum)));
            return false;
        }
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("Failed to CfsWriteBack %s, headerNum: %u.", FilePathName(location.fd),
                               location.headerNum)));
    }

    /* keep nblocks up-to-date in cfsextendextent situation */
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;
    auto limit_n_blocks = location.extentOffset + 1;
    auto autal_n_blocks = pg_atomic_read_u32(&cfsExtentHeader->nblocks);
    while (limit_n_blocks > autal_n_blocks) {
        if (pg_atomic_compare_exchange_u32(&cfsExtentHeader->nblocks, &autal_n_blocks, limit_n_blocks)) {
            pcaNeedFlush = true;
            break;
        }
    }
    Assert (cfsExtentHeader->nblocks > location.extentOffset);

    CfsExtentAddress *cfsExtentAddress = GetExtentAddress(ctrl->pca_page, (uint16)location.extentOffset);
    for (uint8 i = 0; i < cfsExtentAddress->nchunks; ++i) {
        if (firstEnter) {
            seekPosChunk = cfsExtentAddress->chunknos[i];
            lastChunk = seekPosChunk;
            firstEnter = false;
        } else if (cfsExtentAddress->chunknos[i] == lastChunk + 1) {
            lastChunk++;
        } else {
            seekPos = OffsetOfPageCompressChunk(chunkSize, (int)seekPosChunk);
            pc_chunk_number_t nchunks = (lastChunk - seekPosChunk) + 1;
            if (location.is_segment_page) {
                pg_flush_data(location.fd, seekPos, (off_t)nchunks * chunkSize);
            } else {
                FileWriteback(location.fd, seekPos, (off_t) nchunks * chunkSize);
            }

            seekPosChunk = cfsExtentAddress->chunknos[i];
            lastChunk = seekPosChunk;
        }
    }

    /* flush the rest chunks */
    if (!firstEnter) {
        seekPos = (off_t) chunkSize * seekPosChunk;
        pc_chunk_number_t nchunks = (lastChunk - seekPosChunk) + 1;
        if (location.is_segment_page) {
            pg_flush_data(location.fd, seekPos, (off_t)nchunks * chunkSize);
        } else {
            FileWriteback(location.fd, seekPos, (off_t) nchunks * chunkSize);
        }
    }

    pca_buf_free_page(ctrl, location, pcaNeedFlush);
    return true;
}

BlockNumber CfsWriteBack(SMgrRelation reln, const RelFileNode &relNode, int fd, int extent_size, ForkNumber forknum,
                         BlockNumber blocknum, BlockNumber nblocks, EXTEND_STORAGE_TYPE type)
{
    BlockNumber firstSeg = blocknum / CFS_LOGIC_BLOCKS_PER_FILE;
    BlockNumber lastSeg = (blocknum + nblocks - 1) / CFS_LOGIC_BLOCKS_PER_FILE;
    BlockNumber nflush = nblocks;

    if (firstSeg != lastSeg) {
        nflush = CFS_LOGIC_BLOCKS_PER_FILE - (blocknum % CFS_LOGIC_BLOCKS_PER_FILE);
    }

    for (BlockNumber iblock = 0; iblock < nflush; ++iblock) {
        BlockNumber blkwrite = blocknum + iblock;
        ExtentLocation location = g_location_convert[type](reln, relNode, fd, extent_size, forknum, blkwrite);

        /* if it's segment page and we don't compress the page, write the original page. Cases
        that we do this are:
            - It's in 1th or 2th segment logical file that we don't compress blocks by design.
            - It's not data block.
            - It's under a DSS storage.
            - The cfs extent of the block is cross two 1G physical file, punch hole is meaningless. */
        if (location.is_segment_page && (!location.is_compress_allowed)) {
            pg_flush_data(location.fd, blkwrite * BLCKSZ, BLCKSZ);
            ereport(DEBUG2,
                    (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("don't compress block due to it's a slice-acrossed"
                            "block, logicBlockNumber: %u, fd: %d, extent_size: %d, forknum: %d, "
                            "type: %d, RelFileNode.relNode: %d, RelFileNode.opt: %d",
                            blkwrite, fd, extent_size, forknum, type, relNode.relNode, relNode.opt)));
            continue;
        }

#ifdef USE_ASSERT_CHECKING
        if (!location.is_segment_page) {
            Assert(firstSeg == FileSegNo(location.fd));
        }
#endif
        /* @TODO: we'd flush entire extent instead of only flush one page. */
        if (!CfsFlushBlock(reln, forknum, location, blkwrite)) {
            return InvalidBlockNumber;
        }
    }

    return nflush;
}

/** find chunks adjacent to the current chunk that can be reused
 @param[in/out] freeChunkMap  bitmap that recoreds chunk allocation usage.
 @param[in]     targetLength  target continuously reusable chunks needed.
 @param[in]     frontPos      first chunk location for the given block.
 @param[in]     rearPos       last chunk location for the given block.
 @param[in]     maxPos        end position to to find reusable chunks.
 @return if find reusable chunk, return start position, else return INVALID_CHUNK_NUM. */
int CfsFindAdjacentEmptyChunkNo(uint8* freeChunkMap, int targetLength, int frontPos, int rearPos, int maxPos)
{
    int frontCount = 0;
    int rearCount = 0;
    /* chunk start from 1 */
    for (int i = frontPos - 1; i > 0 && i >= frontPos - targetLength; i--) {
        /* 1: use, 0: unuse */
        if (!CFS_BITMAP_GET(freeChunkMap, i)) {
            frontCount++;
        } else {
            break;
        }
    }

    for (int j = rearPos + 1; j <= maxPos && j <= rearPos + targetLength - frontCount; j++) {
        if (!CFS_BITMAP_GET(freeChunkMap, j)) {
            rearCount++;
        } else {
            break;
        }
    }

    if (frontCount + rearCount >= targetLength) {
        if (frontCount > 0) {
            return frontPos - frontCount;
        }
        return frontPos;
    }
    return INVALID_CHUNK_NUM;
}

/** find chunks that can be reused from head position
 @param[in/out] freeChunkMap    bitmap that recoreds chunk allocation usage.
 @param[in]     firstCheckPos   start position to find reusable chunks.
 @param[in]     maxPos          end position to to find reusable chunks.
 @param[in]     targetLength    target continuously reusable chunks needed.
 @return if find reusable chunk, return start position, else return INVALID_CHUNK_NUM.  */
int CfsFindEmptyChunkNo(uint8* freeChunkMap, int firstCheckPos, int maxPos, int targetLength)
{
    int sequenceLength = 0;
    for (int i = firstCheckPos; i <= maxPos; i++) {
        if (!CFS_BITMAP_GET(freeChunkMap, i)) {
            sequenceLength++;
            if (sequenceLength == targetLength) {
                Assert(i - targetLength + 1 > 0);
                Assert(i - targetLength + 1 <= maxPos);
                return i - targetLength + 1;
            }
        } else {
            sequenceLength = 0;
        }
    }
    return INVALID_CHUNK_NUM;
}

/** allocate chunks for block and update
 @param[in/out] cfsExtentAddress  chunk information for a block.
 @param[in]     chunkno           first chunk that start to be allocated.
 @param[in]     needChunks        number of chunks need to be allocated.
 @param[in/out] freeChunkMap      bitmap that recoreds chunk allocation usage. */
static inline void CfsAllocateChunks(CfsExtentAddress *cfsExtentAddress, uint32 chunkno,
                                     uint8 needChunks, uint8* freeChunkMap)
{
    for (int i = 0; i < needChunks; ++i, ++chunkno) {
        Assert(!CFS_BITMAP_GET(freeChunkMap, chunkno));
        CFS_BITMAP_SET(freeChunkMap, chunkno);
        cfsExtentAddress->chunknos[i] = (uint16)chunkno;
    }
}

/** try to allocate new delta chunks from the un-allocated area
 @param[in/out] cfsExtentHeader       pca page header.
 @param[in/out] cfsExtentAddress      chunk information for a block.
 @param[in]     blockAllocateChunks   number of allocated chunks of the block.
 @param[in]     needChunks            number of chunks need to be allocated.
 @param[in/out] freeChunkLock         the lock that control allocated_chunk_usages bitmap.
 @return return true indicates allocated successfully. */
bool ExtendDeltaChunksAtTail(CfsExtentHeader *cfsExtentHeader, CfsExtentAddress *cfsExtentAddress,
                             uint8 blockAllocateChunks, uint8 needChunks, uint8* freeChunkMap)
{
    uint32 chunkno = INVALID_CHUNK_NUM;
    Assert((uint32)cfsExtentHeader->allocated_chunks <=
           (uint32)(BLCKSZ / cfsExtentHeader-> chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));
    chunkno = cfsExtentAddress->chunknos[0];
    if (chunkno != INVALID_CHUNK_NUM) {
        CfsAllocateChunks(cfsExtentAddress, chunkno, needChunks, freeChunkMap);
        Assert(chunkno <= (uint32)(BLCKSZ / cfsExtentHeader-> chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));
        cfsExtentHeader->n_fragment_chunks -= blockAllocateChunks;
        Assert(cfsExtentHeader->n_fragment_chunks <=
               (uint32)(BLCKSZ / cfsExtentHeader-> chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));
        cfsExtentAddress->allocated_chunks = needChunks;
        return true;
    }
    return false;
}

/** if not find adjacent empty chunk, try to find from first position
 @param[in/out] cfsExtentHeader       pca page header.
 @param[in/out] cfsExtentAddress      chunk information for a block.
 @param[in]     blockAllocateChunks   number of allocated chunks of the block.
 @param[in]     needChunks            number of chunks needed to be allocated.
 @param[in]     allocateNumber        number of new chunks needed to be allocated.
 @param[in/out] freeChunkLock         the lock that control allocated_chunk_usages bitmap.
 @return return true indicates allocated successfully. */
bool ReuseAdjacentChunks(CfsExtentHeader *cfsExtentHeader, CfsExtentAddress *cfsExtentAddress,
                         uint8 blockAllocateChunks, uint8 needChunks,
                         int32 allocateNumber, uint8* freeChunkMap)
{
    uint32 chunkno = INVALID_CHUNK_NUM;
    chunkno = CfsFindAdjacentEmptyChunkNo(freeChunkMap, allocateNumber, cfsExtentAddress->chunknos[0],
                                          cfsExtentAddress->chunknos[blockAllocateChunks - 1],
                                          cfsExtentHeader->allocated_chunks);
    if (chunkno != INVALID_CHUNK_NUM) {
        CfsAllocateChunks(cfsExtentAddress, chunkno, needChunks, freeChunkMap);
        Assert(chunkno <= (uint32)(BLCKSZ / cfsExtentHeader-> chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));
        cfsExtentHeader->n_fragment_chunks -= needChunks;
        Assert(cfsExtentHeader->n_fragment_chunks <=
               (uint32)(BLCKSZ / cfsExtentHeader-> chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));
        cfsExtentAddress->allocated_chunks = needChunks;
        return true;
    }
    return false;
}

/** if not find adjacent empty chunk, try to find from the first position
 @param[in/out] cfsExtentHeader   pca page header.
 @param[in/out] cfsExtentAddress  chunk information for a block.
 @param[in]     needChunks        number of chunks needed to be allocated.
 @param[in]     allocateNumber    numebr of new chunks needed to be allocated.
 @param[in/out] freeChunkLock     the lock that control allocated_chunk_usages bitmap.
 @return return true indicates allocated successfully. */
bool ReuseChunksFromHead(CfsExtentHeader *cfsExtentHeader, CfsExtentAddress *cfsExtentAddress,
                         uint8 needChunks, int32 allocateNumber, uint8* freeChunkMap)
{
    uint32 chunkno = INVALID_CHUNK_NUM;
    chunkno = CfsFindEmptyChunkNo(freeChunkMap, CHUNK_START_NUM, cfsExtentHeader->allocated_chunks, needChunks);
    if (chunkno != INVALID_CHUNK_NUM) {
        CfsAllocateChunks(cfsExtentAddress, chunkno, needChunks, freeChunkMap);
        Assert(chunkno <= (uint32)(BLCKSZ / cfsExtentHeader-> chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));
        cfsExtentHeader->n_fragment_chunks -= needChunks;
        Assert(cfsExtentHeader->n_fragment_chunks <=
               (uint32)(BLCKSZ / cfsExtentHeader-> chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));
        cfsExtentAddress->allocated_chunks = needChunks;
        return true;
    }
    return false;
}

/** cannot find empty reuse chunk and its not the last chunk, allocate new chunks for the block
 @param[in/out] cfsExtentHeader    pca page header.
 @param[in/out] cfsExtentAddress   chunk information for a block.
 @param[in]     needChunks         number of chunks needed to be allocated.
 @param[in]     allocateNumber     numebr of new chunks needed to be allocated.
 @param[in/out] freeChunkLock      the lock that control allocated_chunk_usages bitmap.
 @return return true indicates allocated successfully. */
bool ExtendNewChunksAtTail(CfsExtentHeader *cfsExtentHeader, CfsExtentAddress *cfsExtentAddress,
                           uint8 needChunks, int32 allocateNumber, uint8* freeChunkMap)
{
    uint32 chunkno = INVALID_CHUNK_NUM;
    chunkno = (pc_chunk_number_t)pg_atomic_fetch_add_u32(&cfsExtentHeader->allocated_chunks,
                                                         (uint32)needChunks) + 1;
    CfsAllocateChunks(cfsExtentAddress, chunkno, needChunks, freeChunkMap);
    Assert(chunkno <= (uint32)(BLCKSZ / cfsExtentHeader->chunk_size * CFS_LOGIC_BLOCKS_PER_EXTENT));
    cfsExtentAddress->allocated_chunks = needChunks;
    return true;
}

static bool ExtendChunksOfBlockCore(CfsExtentHeader *cfsExtentHeader,
                                    CfsExtentAddress *cfsExtentAddress,
                                    uint8 needChunks, uint8 actualUse, LWLock *freeChunkLock)
{
    bool res = false;
    if (g_instance.attr.attr_storage.enable_tpc_fragment_chunks) {
        if (freeChunkLock != NULL) {
            (void)LWLockAcquire(freeChunkLock, LW_EXCLUSIVE);
        }
        int32 allocateNumber = needChunks - cfsExtentAddress->allocated_chunks;
        if (allocateNumber > 0) {
            cfsExtentHeader->recycleInOrder = 0;
            uint8 blockAllocateChunks = cfsExtentAddress->allocated_chunks;
            uint32 rearchunk = INVALID_CHUNK_NUM;
            if (blockAllocateChunks > 0) {
                rearchunk = (uint32)cfsExtentAddress->chunknos[blockAllocateChunks - 1];
            }

            uint8* freeChunkMap = cfsExtentHeader->allocated_chunk_usages;
            /* clean up the current chunks */
            for (int i = 0; i < blockAllocateChunks; i++) {
                Assert(CFS_BITMAP_GET(freeChunkMap, cfsExtentAddress->chunknos[i]));
                CFS_BITMAP_CLEAR(freeChunkMap, cfsExtentAddress->chunknos[i]);
            }
            cfsExtentHeader->n_fragment_chunks += blockAllocateChunks;
            cfsExtentAddress->allocated_chunks = 0;
            /* situation 1: try to allocate new delta chunks from the un-allocated area */
            if (blockAllocateChunks > 0 &&
                pg_atomic_compare_exchange_u32(&cfsExtentHeader->allocated_chunks,
                &rearchunk, cfsExtentHeader->allocated_chunks + allocateNumber)) {
                res = ExtendDeltaChunksAtTail(cfsExtentHeader, cfsExtentAddress, blockAllocateChunks,
                                              needChunks, freeChunkMap);
            }

            /* situation 2: if fail to allocate new chunks, try to reuse fragment to assemble a new continuous chunks */
            if (!res && blockAllocateChunks > 0) {
                res = ReuseAdjacentChunks(cfsExtentHeader, cfsExtentAddress, blockAllocateChunks,
                                          needChunks, allocateNumber, freeChunkMap);
            }

            /* situation 3: if not find adjacent empty chunk, try to find from the first position */
            if (!res && cfsExtentHeader->n_fragment_chunks >= needChunks) {
                res = ReuseChunksFromHead(cfsExtentHeader, cfsExtentAddress, needChunks,
                                          allocateNumber, freeChunkMap);
            }

            /* situation 4: cannot find empty reuse chunk and its not the last chunk, allocate new chunks for the block */
            if (!res) {
                res = ExtendNewChunksAtTail(cfsExtentHeader, cfsExtentAddress, needChunks,
                                            allocateNumber, freeChunkMap);
            }
        }

        if (freeChunkLock != NULL) {
            LWLockRelease(freeChunkLock);
        }
    } else {
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

static bool ExtendChunksOfBlock(CfsExtentHeader *cfsExtentHeader, ExtentLocation *location, uint8 needChunks,
                                uint8 actualUse, LWLock *freeChunkLock)
{
    auto cfsExtentAddress = GetExtentAddress(cfsExtentHeader, (uint16)location->extentOffset);
    return ExtendChunksOfBlockCore(cfsExtentHeader, cfsExtentAddress, needChunks, actualUse, freeChunkLock);
}

/** chunk recycle threshold: 1/4 of chunks is empty
 @param[in] cfsExtentHeader pca page header.
 @return threshold for doing extent recycle. */
static inline uint16 RecycleChunkThreshold(CfsExtentHeader *cfsExtentHeader)
{
    return (CFS_LOGIC_BLOCKS_PER_EXTENT * BLCKSZ) / cfsExtentHeader->chunk_size / 4;
}

/** maximun number of chunks for doing recycle.
 @param[in] cfsExtentHeader pca page header.
 @return maximun number of chunks for doing recycle. */
static inline uint32 MaxChunkNumForRecycle(CfsExtentHeader *cfsExtentHeader)
{
    /* reserve one page for recycle threshold, to avoid the entire extent is uncompressed. */
    return (CFS_LOGIC_BLOCKS_PER_EXTENT - 1)* BLCKSZ / cfsExtentHeader->chunk_size;
}

/** the number of chunks for per block.
 @param[in] cfsExtentHeader pca page header.
 @return the number of chunks for per block. */
static inline uint32 SingleBlockChunkNumForRecycle(CfsExtentHeader *cfsExtentHeader)
{
    return BLCKSZ / cfsExtentHeader->chunk_size;
}

size_t CfsWritePage(SMgrRelation reln, const RelFileNode &relNode, int fd, int extent_size,
                    ForkNumber forknum, BlockNumber logicBlockNumber, const char *buffer,
                    bool isExtend, EXTEND_STORAGE_TYPE type)
{
    /* Normally, when extend page, it meats PageIsNew(buffer) here, so we quick return if page,
    since zero pages have no need to get any chunk in pcd. And another scene is that when
    enable_adio_function and enable_fast_allocate is on, the caller such as rewrite_flush_page may
    want md functions to use fallocate and pass nullptr buffer down here, it also means extend a
    new page scene, so for page compress, we have nothing to do here but return BLCKSZ. Note that
    enable_adio_function is not allowed to be turned on now, we just considered it anyway. */
    if (buffer == nullptr || PageIsNew(buffer)) {
        return BLCKSZ;
    }
    int nbytes;
    ExtentLocation location =
        g_location_convert[type](reln, relNode, fd, extent_size, forknum, logicBlockNumber);
    /* if it's segment page and we don't compress the page, write the original page. Cases
    that we do this are:
        - It's in 1th or 2th segment logical file that we don't compress blocks by design.
        - It's not data block.
        - It's under a DSS storage.
        - The cfs extent of the block is cross two 1G physical file, punch hole is meaningless. */
    if (location.is_segment_page && (!location.is_compress_allowed)) {
        nbytes = DirectFilePWrite(location.fd, buffer, BLCKSZ, location.GetBlockPhysicalOffset(),
                                  (uint32)WAIT_EVENT_DATA_FILE_WRITE);
        ereport(DEBUG2, (errmsg("CfsWritePage with out compress, logicBlockNumber: %u, fd: %d, extent_size: %d,"
                                "forknum: %d,type: %d, RelFileNode.relNode: %d, RelFileNode.opt: %d",
                                logicBlockNumber, fd, extent_size, forknum, type, relNode.relNode, relNode.opt)));
        if (nbytes != BLCKSZ) {
            ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
                    errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes", logicBlockNumber,
                           FilePathName(location.fd), nbytes, BLCKSZ), errhint("Check free disk space.")));
        }
        return BLCKSZ;
    }
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
    TransCompressOptions(relNode, &option);
    uint16 chunkSize = cfsExtentHeader->chunk_size;

    /* compress page */
    uint8 nchunks;
    char *compressedBuffer;
    /* if it's segment page and we don't compress the page, write the original page. Cases
    that we do this are:
        - It's in 1th or 2th segment logical file that we don't compress blocks by design.
        - It's not data block.
        - It's under a DSS storage.
        - The cfs extent of the block is cross two 1G physical file, punch hole is meaningless. */
    if (location.is_segment_page && (!location.is_compress_allowed)) {
        compressedBuffer = static_cast<char *>(palloc((unsigned long)BLCKSZ));
        errno_t rc = memcpy_s(compressedBuffer, BLCKSZ, buffer, BLCKSZ);
        securec_check(rc, "\0", "\0");
        nchunks = (uint8)(BLCKSZ / location.chunk_size);
    } else {
        compressedBuffer = CfsCompressPage(buffer, &option, &nchunks);
    }

    /* set address */
    uint8 need_chunks = option.compressPreallocChunks > nchunks ? (uint8)option.compressPreallocChunks : (uint8)nchunks;
    bool changed = ExtendChunksOfBlock(cfsExtentHeader, &location, need_chunks, nchunks,
                                       ctrl->allocated_chunk_usages_lock);

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
        if (location.is_segment_page) {
            nbytes = DirectFilePWrite(location.fd, buffer_pos, write_amount, seekPos + extentStartOffset,
                                      (uint32)WAIT_EVENT_DATA_FILE_WRITE);
        } else {
            nbytes = FilePWrite(location.fd, buffer_pos, write_amount, seekPos + extentStartOffset,
                                (uint32)WAIT_EVENT_DATA_FILE_WRITE);
        }
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

    /* keep nblocks up-to-date in cfsextendextent situation */
    auto limit_n_blocks = location.extentOffset + 1;
    auto autal_n_blocks = pg_atomic_read_u32(&cfsExtentHeader->nblocks);

    while (limit_n_blocks > autal_n_blocks) {
        if (pg_atomic_compare_exchange_u32(&cfsExtentHeader->nblocks, &autal_n_blocks, limit_n_blocks)) {
            isExtend = true;
            break;
        }
    }
    
    Assert (cfsExtentHeader->nblocks > location.extentOffset);

    /* free compressed buffer */
    if (compressedBuffer != NULL && compressedBuffer != buffer) {
        pfree(compressedBuffer);
    }

    pca_buf_free_page(ctrl, location, changed || isExtend);

    /* try recyle extent */
    if (g_instance.attr.attr_storage.enable_tpc_fragment_chunks) {
        if (cfsExtentHeader->n_fragment_chunks > RecycleChunkThreshold(cfsExtentHeader) ||
            (cfsExtentHeader->allocated_chunks >= MaxChunkNumForRecycle(cfsExtentHeader) &&
            cfsExtentHeader->n_fragment_chunks >= SingleBlockChunkNumForRecycle(cfsExtentHeader))) {
            CfsRecycleOneExtent(reln, forknum, &location);
            ereport(LOG, (errmsg("Complete a recycle operation, relNode: %u, extentNumebr: %d",
                                 location.relFileNode.relNode, location.extentNumber)));
        }
    }

    return BLCKSZ;
}

void InitExtentHeader(const ExtentLocation& location)
{
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NO_READ);
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    errno_t rc =  memset_s((char *)cfsExtentHeader, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");

    cfsExtentHeader->algorithm = location.algorithm;
    cfsExtentHeader->chunk_size = location.chunk_size;
    /* now ctrl->load_status is not set, and the disk pca page is not written, and the caller
    may read pca before calling pca_buf_free_page, so we should pca_buf_free_page here. */
    pca_buf_free_page(ctrl, location, true);
}

void CfsExtendForSeg(const RelFileNode& relNode, int fd, int extent_size, ForkNumber forknum,
                     BlockNumber logicBlockNumber, const char *buffer, const ExtentLocation &location)
{
    if (location.fd < 0) {
        return;
    }
    ereport(DEBUG2,
        (errmsg("[sgement compress]CfsExtendForSeg: %u, fd: %d, extent_size: %d, forknum: %d,type: %d,"
                "RelFileNode.relNode: %d, RelFileNode.opt: %d",
                logicBlockNumber, fd, extent_size, forknum, SEG_STORAGE, relNode.relNode,
                relNode.opt)));
    if (location.extentOffset == 0) {
        /* extend and fallocate */
        auto start = location.extentStart * BLCKSZ;
        InitExtentHeader(location);
        /* if it's segment page and we don't compress the page, write the original page. Cases
        that we do this are:
            - It's in 1th or 2th segment logical file that we don't compress blocks by design.
            - It's not data block.
            - It's under a DSS storage.
            - The cfs extent of the block is cross two 1G physical file, punch hole is meaningless. */
        if (location.is_segment_page && location.is_compress_allowed) {
            FilePunchHoleAlloc(location.fd, start, CFS_LOGIC_BLOCKS_PER_EXTENT * BLCKSZ);
        }
        ereport(DEBUG2, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("[sgement compress]InitExtentHeader relNode relFileNode: %d,"
                       " opt: %d, headerNum: %u.",
                       relNode.relNode, relNode.opt, location.headerNum)));
    }
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("[sgement compress]Failed to load pca during extent, fd: %d, headerNum: %u.",
                       location.fd, location.headerNum)));
    }
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    auto limit_n_blocks = location.extentOffset + 1;
    auto autal_n_blocks = pg_atomic_read_u32(&cfsExtentHeader->nblocks);
    while (limit_n_blocks > autal_n_blocks &&
           (!pg_atomic_compare_exchange_u32(&cfsExtentHeader->nblocks,
                                            &autal_n_blocks, limit_n_blocks)));

    pca_buf_free_page(ctrl, location, true);
}

void CfsExtendExtent(SMgrRelation reln, const RelFileNode &relNode, int fd, int extent_size, ForkNumber forknum,
                     BlockNumber logicBlockNumber, const char *buffer, EXTEND_STORAGE_TYPE type)
{
    ExtentLocation location =
        g_location_convert[type](reln, relNode, fd, extent_size, forknum, logicBlockNumber);
    if (location.fd < 0) {
        return;
    }

    Assert(!location.is_segment_page && location.is_compress_allowed);

    if (location.extentOffset == 0) {
        /* extend and fallocate */
        auto start = location.extentStart * BLCKSZ;
        InitExtentHeader(location);
        if (!location.is_segment_page) {
            FileAllocate(location.fd, start, CFS_LOGIC_BLOCKS_PER_EXTENT * BLCKSZ);
        }
    }
    (void)CfsWritePage(reln, relNode, fd, extent_size, forknum, logicBlockNumber, buffer, true, type);
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_SHARED, PCA_BUF_NORMAL_READ);
    if (ctrl->load_status == CTRL_PAGE_LOADED_ERROR) {
        pca_buf_free_page(ctrl, location, false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsExtendExtent %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
    }
    CfsExtentHeader *cfsExtentHeader = ctrl->pca_page;

    auto limit_n_blocks = location.extentOffset + 1;
    auto autal_n_blocks = pg_atomic_read_u32(&cfsExtentHeader->nblocks);
    while (limit_n_blocks > autal_n_blocks &&
           (!pg_atomic_compare_exchange_u32(&cfsExtentHeader->nblocks, &autal_n_blocks, limit_n_blocks)));
    Assert (cfsExtentHeader->nblocks > location.extentOffset);

    pca_buf_free_page(ctrl, location, true);
    return;
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

    ExtentLocation location = {
        .fd = fd,
        .relFileNode = relFileNode,
        .extentNumber = (extentCount - 1) + segNo * CFS_EXTENT_COUNT_PER_FILE,
        .extentStart = 0, /* unused */
        .extentOffset = 0,
        .headerNum = extentCount * CFS_EXTENT_SIZE - 1,
        .chunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize],
        .algorithm = (uint8)option.compressAlgorithm
    };

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

void CfsMdPrefetch(SMgrRelation reln, const RelFileNode &relNode, int fd, int extent_size, ForkNumber forknum,
                   BlockNumber logicBlockNumber, EXTEND_STORAGE_TYPE type)
{
    ExtentLocation location =
        g_location_convert[type](reln, relNode, fd, extent_size, forknum, logicBlockNumber);
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

off_t CfsMdTruncate(SMgrRelation reln, const RelFileNode &relNode, int fd, int extent_size, ForkNumber forknum,
                    BlockNumber logicBlockNumber, EXTEND_STORAGE_TYPE type)
{
    ExtentLocation location =
        g_location_convert[type](reln, relNode, fd, extent_size, forknum, logicBlockNumber);
    /* if logicBlockNumber is the first block of extent, truncate all after blocks */
    if (logicBlockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT == 0) {
        return location.extentStart * BLCKSZ;  // ok
    }

    auto truncateOffset = (location.headerNum + 1) * BLCKSZ;
    if (location.is_segment_page) {
        return truncateOffset;
    }
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
    uint32 len = (uint32)((CFS_MAX_LOGIC_CHUNKS_NUMBER(cfsExtentHeader->chunk_size) - max) *
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
    int fd = CfsGetFd(reln, MAIN_FORKNUM, logicBlockNumber, true, EXTENT_OPEN_FILE);
    /* The relation has been removed, nothing to do here. */
    if (fd < 0) {
        return;
    }
    ExtentLocation location =
        g_location_convert[COMMON_STORAGE](reln, reln->smgr_rnode.node, fd, CFS_EXTENT_SIZE,
                                           MAIN_FORKNUM, logicBlockNumber);

    /* load the pca page directly */
    CfsExtentHeader *pca_disk = (CfsExtentHeader *)palloc0(BLCKSZ);
    CfsExtentHeader *pca_mem = (CfsExtentHeader *)palloc0(BLCKSZ);
    int nbytes = FilePRead(location.fd, (char *)pca_disk, BLCKSZ, location.GetPcaPhysicalOffset(),
                           (uint32)WAIT_EVENT_DATA_FILE_READ);
    if (nbytes != BLCKSZ) {
        pfree(pca_disk);
        pfree(pca_mem);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to CfsHeaderPageCheckAndRepair %s", FilePathName(location.fd))));
    }

    /* lock the extent by LW_EXCLUSIVE */
    pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_EXCLUSIVE, PCA_BUF_NO_READ);

    CfsHeaderPagerCheckStatus disk_status =
        CheckAndRepairCompressAddress(pca_disk, location.chunk_size, location.algorithm, location);
    CfsHeaderPagerCheckStatus mem_status = CFS_HEADER_CHECK_STATUS_ERROR;
    if (ctrl->load_status == CTRL_PAGE_IS_LOADED) {
        rc = memcpy_s(pca_mem, BLCKSZ, ctrl->pca_page, BLCKSZ);
        securec_check(rc, "\0", "\0");
        mem_status = CheckAndRepairCompressAddress(pca_mem, location.chunk_size,
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
static void WriteRepairFile_Compress_Block(RelFileCompressOption option, char *buf, char *compressed_buf,
                                           CfsExtentAddress *cfsExtentAddress, CfsExtentHeader *cfsExtentHeader)
{
    errno_t rc = 0;
    uint16 chunkSize = cfsExtentHeader->chunk_size;

    /* compress page */
    uint8 nchunks;
    char *compressedBuffer = CfsCompressPage(buf, &option, &nchunks);

    /* set address */
    uint8 need_chunks = option.compressPreallocChunks > nchunks ? (uint8)option.compressPreallocChunks : (uint8)nchunks;
    (void)ExtendChunksOfBlockCore(cfsExtentHeader, cfsExtentAddress, need_chunks, nchunks, NULL);

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
        (CFS_MAX_LOGIC_CHUNKS_NUMBER(cfsExtentHeader->chunk_size) - max_chrunkno) * cfsExtentHeader->chunk_size;
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
        g_location_convert[COMMON_STORAGE](reln, reln->smgr_rnode.node, fd, CFS_EXTENT_SIZE, MAIN_FORKNUM,
                                           logicBlockNumber);

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
static void CfsWriteFile(int fd, const void *buf, int size, int64 offset)
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

    int fd = CfsGetFd(reln, forknum, blocknum, skipFsync, WRITE_BACK_OPEN_FILE);
    /* The relation has been removed, nothing to do here. */
    if (fd < 0) {
        return;
    }
    /* buffer init is ahead of dw, buffer can be used for reading pca */
    ExtentLocation location =
        StorageConvert(reln, reln->smgr_rnode.node, fd, CFS_EXTENT_SIZE, forknum, blocknum);
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

    int fd = CfsGetFd(relation, extInfo->forknum,
        extInfo->extentNumber * CFS_LOGIC_BLOCKS_PER_EXTENT, false, WRITE_BACK_OPEN_FILE);

    /* The relation has been removed, nothing to do here. */
    if (fd < 0) {
        return;
    }

    ExtentLocation location =
        StorageConvert(relation, relation->smgr_rnode.node, fd, CFS_EXTENT_SIZE, extInfo->forknum,
                       extInfo->extentNumber * CFS_LOGIC_BLOCKS_PER_EXTENT);
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
    if (location.is_segment_page) {
        nbytes = DirectFilePWrite(location.fd, aligned_buf, CFS_EXTENT_SIZE * BLCKSZ,
                                  location.extentStart * BLCKSZ, (uint32)WAIT_EVENT_DATA_FILE_WRITE);
    } else {
        nbytes = FilePWrite(location.fd, aligned_buf, CFS_EXTENT_SIZE * BLCKSZ,
                            location.extentStart * BLCKSZ, (uint32)WAIT_EVENT_DATA_FILE_WRITE);
    }
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
        .chunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize],
        .algorithm = (uint8)option.compressAlgorithm
    };
}

static void CfsSortOutChunk(const ExtentLocation &location, CfsExtentHeader *srcExtPca, char *alignbuf)
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

static void CfsPunchHole(const ExtentLocation &location, CfsExtentHeader *assistPca)
{
    uint32 chunksum = assistPca->allocated_chunks;

    uint32 punchOffset = chunksum * assistPca->chunk_size;
    /* alignment of 4K, the uint of punch hole */
    punchOffset = TYPEALIGN(MIN_FALLOCATE_SIZE, punchOffset);
    uint32 start = location.extentStart * BLCKSZ + punchOffset;
    uint32 punchSize = location.GetPcaPhysicalOffset() - location.extentStart * BLCKSZ - punchOffset;

    if (punchSize == 0) {
        return;
    }

    if (location.is_segment_page) {
        FilePunchHoleAlloc(location.fd, start, punchSize);
    } else {
        FileAllocate(location.fd, start, punchSize);
    }
}

/* recycle chunk in ext */
static void CfsRecycleChunkInExt(ExtentLocation location, int assistfd, char *alignbuf,
                                 pca_page_ctrl_t *ctrl)
{
    char *srcBuf = alignbuf; // src buf includes 128 pages
    char *assistBuf = srcBuf + CFS_EXTENT_SIZE * BLCKSZ - BLCKSZ; // assist includes 129 page;
    char *assistInfobuf = assistBuf + CFS_EXTENT_SIZE * BLCKSZ; // assist info is stored in last added page
    int nbytes;

    CfsExtInfo *extInfo = (CfsExtInfo *)(void *)assistInfobuf;
    location.extentNumber = extInfo->extentNumber;
    location.extentStart = (extInfo->extentNumber % CFS_EXTENT_COUNT_PER_FILE) * CFS_EXTENT_SIZE;
    location.headerNum = location.extentStart + CFS_LOGIC_BLOCKS_PER_EXTENT;

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

    if (location.is_segment_page) {
        nbytes =
            DirectFilePWrite(location.fd, assistBuf, CFS_EXTENT_SIZE * BLCKSZ, location.extentStart * BLCKSZ,
                             (uint32)WAIT_EVENT_DATA_FILE_WRITE);
    } else {
        nbytes = FilePWrite(location.fd, assistBuf, CFS_EXTENT_SIZE * BLCKSZ, location.extentStart * BLCKSZ,
                            (uint32)WAIT_EVENT_DATA_FILE_WRITE);
    }

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

    if (g_instance.attr.attr_storage.enable_tpc_fragment_chunks) {
        /* lock free chunk bitmap */
        (void)LWLockAcquire(ctrl->allocated_chunk_usages_lock, LW_EXCLUSIVE);
        CfsExtentAddress *extAddr = NULL;

        rc = memset_s(ctrl->pca_page->allocated_chunk_usages, ALLOCATE_CHUNK_USAGE_LEN,
                      0, ALLOCATE_CHUNK_USAGE_LEN);
        securec_check(rc, "\0", "\0");

        /* recount n_fragment_chunks */
        uint16 usedChunkCount = 0;
        for (uint16 i = 0; i < ctrl->pca_page->nblocks; i++) {
            extAddr = GetExtentAddress(ctrl->pca_page, i);
            for (int j = 0; j < extAddr->allocated_chunks; j++) {
                CFS_BITMAP_SET(ctrl->pca_page->allocated_chunk_usages, extAddr->chunknos[j]);
                usedChunkCount++;
            }
        }
        ctrl->pca_page->n_fragment_chunks = ctrl->pca_page->allocated_chunks - usedChunkCount;
        LWLockRelease(ctrl->allocated_chunk_usages_lock);
    }

    /* free pca buffer */
    PCA_SET_NO_READ(ctrl);
    pca_buf_free_page(ctrl, location, false);
}

static void CfsRecycleOneExtent(SMgrRelation reln, ForkNumber forknum, ExtentLocation *oldLocation)
{
    ExtentLocation location = FormExtLocation(reln, 0);
    char filePath[MAXPGPATH];

    /* lock pca, protect ext from reading and writing */
    pca_page_ctrl_t *ctrl = pca_buf_read_page(*oldLocation, LW_EXCLUSIVE, PCA_BUF_NORMAL_READ);
    errno_t rc = snprintf_s(filePath, MAXPGPATH, MAXPGPATH - 1,
        "global/pg_dw_ext_chunk/%u_%u_assist_tmp", reln->smgr_rnode.node.relNode, oldLocation->extentNumber);
    securec_check_ss(rc, "\0", "\0");

    int assistfd = CfsCreateFile(filePath);

    /* alloc buffer for pca and assist pca */
    char *unaligned_buf = (char *) palloc(CFS_PCA_AND_ASSIST_BUFFER_SIZE + BLCKSZ);
    char *aligned_buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    /* assist info is stored in last added page */
    char *assistInfobuf = aligned_buf + CFS_PCA_AND_ASSIST_BUFFER_SIZE - BLCKSZ;

    rc = memset_s(aligned_buf, CFS_PCA_AND_ASSIST_BUFFER_SIZE, 0, CFS_PCA_AND_ASSIST_BUFFER_SIZE);
    securec_check(rc, "\0", "\0");

    CfsExtInfo *extInfo = (CfsExtInfo *)(void *)assistInfobuf;
    extInfo->forknum = forknum;
    extInfo->rnode = reln->smgr_rnode.node;

    location.fd = oldLocation->fd;
    /* recyle blocks at the extent granularity (128 pcd) */
    extInfo->extentNumber = oldLocation->extentNumber;
    CfsRecycleChunkInExt(location, assistfd, aligned_buf, ctrl);
    fsync(location.fd);
    pfree(unaligned_buf);
    (void)close(assistfd);
    CfsRemoveFile(filePath);
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
            /* lock pca, protect ext from reading and writing */
            pca_page_ctrl_t *ctrl = pca_buf_read_page(location, LW_EXCLUSIVE, PCA_BUF_NORMAL_READ);
            CfsRecycleChunkInExt(location, assistfd, aligned_buf, ctrl);
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
    if(IsSegmentFileNode(reln->smgr_rnode.node)) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED),
                errmsg("shrink table cmd is not supported for segment table.")));
    }
    /* it must be compressed file at now. */
    if (reln == NULL || !IS_COMPRESSED_MAINFORK(reln, forknum)) {
        return;
    }
    /* return until the massion is done */
    CfsRecycleChunkProc(reln, forknum);
}

int CfsGetFd(SMgrRelation sRel, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync, int type)
{
    int fd = -1;
    MdfdVec *v = NULL;
    if (type == EXTENT_OPEN_FILE) {
        v = _mdfd_getseg(sRel, forknum, logicBlockNumber, skipSync, EXTENSION_FAIL);
    } else if (type == WRITE_BACK_OPEN_FILE) {
        v = _mdfd_getseg(sRel, forknum, logicBlockNumber, skipSync, EXTENSION_RETURN_NULL);
    } else if (type == EXTENT_CREATE_FILE) {
        v = _mdfd_getseg(sRel, forknum, logicBlockNumber, skipSync, EXTENSION_CREATE);
    }
    if (v != NULL) {
        fd = v->mdfd_vfd;
    }
    return fd;
}
