/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 * Copyright (c) 2020, PostgreSQL Global Development Group
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
 * page_compression.cpp
 *	  Routines for page compression
 *
 * There are two implementations at the moment: zstd, and the Postgres
 * pg_lzcompress(). zstd support requires that the server was compiled
 * with --with-zstd.
 * IDENTIFICATION
 *    ./src/gausskernel/storage/smgr/page_compression.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "utils/datum.h"
#include "utils/relcache.h"

#include "utils/timestamp.h"
#include "storage/checksum.h"
#include "storage/page_compression.h"
#include "storage/page_compression_impl.h"

static void CheckHeaderOfCompressAddr(PageCompressHeader* pcMap, uint16 chunk_size, uint8 algorithm, const char* path)
{
    if (pcMap->chunk_size != chunk_size || pcMap->algorithm != algorithm) {
        if (u_sess->attr.attr_security.zero_damaged_pages) {
            ereport(WARNING,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid chunk_size %u or algorithm %u in head of compress relation address file \"%s\", "
                           "and reinitialized it.",
                        pcMap->chunk_size,
                        pcMap->algorithm,
                        path)));

            pcMap->algorithm = algorithm;
            pg_atomic_write_u32(&pcMap->nblocks, RELSEG_SIZE);
            pg_atomic_write_u32(&pcMap->allocated_chunks, 0);
            pg_atomic_write_u32(&pcMap->last_synced_allocated_chunks, 0);
            pcMap->chunk_size = chunk_size;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid chunk_size %u or algorithm %u in head of compress relation address file \"%s\"",
                        pcMap->chunk_size,
                        pcMap->algorithm,
                        path)));
        }
    }
}

void CheckAndRepairCompressAddress(PageCompressHeader *pcMap, uint16 chunk_size, uint8 algorithm, const char *path)
{
    TimestampTz lastRecoveryTime = pcMap->last_recovery_start_time;
    TimestampTz pgStartTime = t_thrd.time_cxt.pg_start_time;
    error_t rc;
    /* if the relation had been checked in this startup, skip */
    if (lastRecoveryTime == pgStartTime) {
        return;
    }

    /* check head of compress address file */
    CheckHeaderOfCompressAddr(pcMap, chunk_size, algorithm, path);

    uint32 nblocks = pg_atomic_read_u32(&pcMap->nblocks);
    uint32 allocated_chunks = pg_atomic_read_u32(&pcMap->allocated_chunks);
    BlockNumber *global_chunknos = (BlockNumber *)palloc0(MAX_CHUNK_NUMBER(chunk_size) * sizeof(BlockNumber));

    BlockNumber max_blocknum = (BlockNumber)-1;
    BlockNumber max_nonzero_blocknum = (BlockNumber)-1;
    BlockNumber max_allocated_chunkno = (pc_chunk_number_t)0;

    /* check compress address of every pages */
    for (BlockNumber blocknum = 0; blocknum < (BlockNumber)RELSEG_SIZE; ++blocknum) {
        PageCompressAddr *pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blocknum);
        if (pcAddr->checksum != AddrChecksum32(blocknum, pcAddr, chunk_size)) {
            ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid checkum %u of block %u in file \"%s\"",
                                                                      pcAddr->checksum, blocknum, path)));
            pcAddr->allocated_chunks = pcAddr->nchunks = 0;
            for (int i = 0; i < BLCKSZ / chunk_size; ++i) {
                pcAddr->chunknos[i] = 0;
            }
            pcAddr->checksum = 0;
        }
        /*
         * skip when found first zero filled block after nblocks
         * if(blocknum >= (BlockNumber)nblocks && pcAddr->allocated_chunks == 0)
         * break;
         */

        /* check allocated_chunks for one page */
        if (pcAddr->allocated_chunks > BLCKSZ / chunk_size) {
            if (u_sess->attr.attr_security.zero_damaged_pages) {
                rc = memset_s((void *)pcAddr, SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size), 0,
                              SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size));
                securec_check_c(rc, "\0", "\0");
                ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                  errmsg("invalid allocated_chunks %u of block %u in file \"%s\", and zero this block",
                                         pcAddr->allocated_chunks, blocknum, path)));
                continue;
            } else {
                pfree(global_chunknos);
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                                errmsg("invalid allocated_chunks %u of block %u in file \"%s\"",
                                       pcAddr->allocated_chunks, blocknum, path)));
            }
        }

        /* check chunknos for one page */
        for (int i = 0; i < pcAddr->allocated_chunks; ++i) {
            /* check for invalid chunkno */
            if (pcAddr->chunknos[i] == 0 || pcAddr->chunknos[i] > MAX_CHUNK_NUMBER(chunk_size)) {
                if (u_sess->attr.attr_security.zero_damaged_pages) {
                    rc = memset_s((void *)pcAddr, SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size), 0,
                                  SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size));
                    securec_check_c(rc, "\0", "\0");
                    ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                      errmsg("invalid chunk number %u of block %u in file \"%s\", and zero this block",
                                             pcAddr->chunknos[i], blocknum, path)));
                    continue;
                } else {
                    pfree(global_chunknos);
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                                    errmsg("invalid chunk number %u of block %u in file \"%s\"", pcAddr->chunknos[i],
                                           blocknum, path)));
                }
            }

            /* check for duplicate chunkno */
            if (global_chunknos[pcAddr->chunknos[i] - 1] != 0) {
                if (u_sess->attr.attr_security.zero_damaged_pages) {
                    rc = memset_s((void *)pcAddr, SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size), 0,
                                  SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size));
                    securec_check_c(rc, "\0", "\0");
                    ereport(
                        WARNING,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                         errmsg(
                             "chunk number %u of block %u duplicate with block %u in file \"%s\", and zero this block",
                             pcAddr->chunknos[i], blocknum, global_chunknos[pcAddr->chunknos[i] - 1], path)));
                    continue;
                } else {
                    pfree(global_chunknos);
                    ereport(ERROR,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                             errmsg("chunk number %u of block %u duplicate with block %u in file \"%s\"",
                                    pcAddr->chunknos[i], blocknum, global_chunknos[pcAddr->chunknos[i] - 1], path)));
                }
            }
        }

        /* clean chunknos beyond allocated_chunks for one page */
        for (int i = pcAddr->allocated_chunks; i < BLCKSZ / chunk_size; ++i) {
            if (pcAddr->chunknos[i] != 0) {
                pcAddr->chunknos[i] = 0;
                ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                  errmsg("clear chunk number %u beyond allocated_chunks %u of block %u in file \"%s\"",
                                         pcAddr->chunknos[i], pcAddr->allocated_chunks, blocknum, path)));
            }
        }

        /* check nchunks for one page */
        if (pcAddr->nchunks > pcAddr->allocated_chunks) {
            if (u_sess->attr.attr_security.zero_damaged_pages) {
                rc = memset_s((void *)pcAddr, SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size), 0,
                              SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size));
                securec_check_c(rc, "\0", "\0");
                ereport(
                    WARNING,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("nchunks %u exceeds allocated_chunks %u of block %u in file \"%s\", and zero this block",
                            pcAddr->nchunks, pcAddr->allocated_chunks, blocknum, path)));
                continue;
            } else {
                pfree(global_chunknos);
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                                errmsg("nchunks %u exceeds allocated_chunks %u of block %u in file \"%s\"",
                                       pcAddr->nchunks, pcAddr->allocated_chunks, blocknum, path)));
            }
        }

        max_blocknum = blocknum;
        if (pcAddr->nchunks > 0) {
            max_nonzero_blocknum = blocknum;
        }

        for (int i = 0; i < pcAddr->allocated_chunks; ++i) {
            global_chunknos[pcAddr->chunknos[i] - 1] = blocknum + 1;
            if (pcAddr->chunknos[i] > max_allocated_chunkno) {
                max_allocated_chunkno = pcAddr->chunknos[i];
            }
        }
    }

    int unused_chunks = 0;
    /* check for holes in allocated chunks */
    for (BlockNumber i = 0; i < max_allocated_chunkno; i++) {
        if (global_chunknos[i] == 0) {
            unused_chunks++;
        }
    }

    if (unused_chunks > 0) {
        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                          errmsg("there are %u chunks of total allocated chunks %u can not be use in file \"%s\"",
                                 unused_chunks, max_allocated_chunkno, path),
                          errhint("You may need to run VACUMM FULL to optimize space allocation.")));
    }

    /* update nblocks in head of compressed file */
    if (nblocks < max_nonzero_blocknum + 1) {
        pg_atomic_write_u32(&pcMap->nblocks, max_nonzero_blocknum + 1);
        pg_atomic_write_u32(&pcMap->last_synced_nblocks, max_nonzero_blocknum + 1);

        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                          errmsg("update nblocks head of compressed file \"%s\". old: %u, new: %u", path, nblocks,
                                 max_nonzero_blocknum + 1)));
    }

    /* update allocated_chunks in head of compress file */
    if (allocated_chunks != max_allocated_chunkno) {
        pg_atomic_write_u32(&pcMap->allocated_chunks, max_allocated_chunkno);
        pg_atomic_write_u32(&pcMap->last_synced_allocated_chunks, max_allocated_chunkno);

        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                          errmsg("update allocated_chunks in head of compressed file \"%s\". old: %u, new: %u", path,
                                 allocated_chunks, max_allocated_chunkno)));
    }

    /* clean compress address after max_blocknum + 1 */
    for (BlockNumber blocknum = max_blocknum + 1; blocknum < (BlockNumber)RELSEG_SIZE; blocknum++) {
        char buf[128];
        char *p = NULL;
        PageCompressAddr *pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blocknum);

        /* skip zero block */
        if (pcAddr->allocated_chunks == 0 && pcAddr->nchunks == 0) {
            continue;
        }

        /* clean compress address and output content of the address */
        rc = memset_s(buf, sizeof(buf), 0, sizeof(buf));
        securec_check_c(rc, "\0", "\0");
        p = buf;

        for (int i = 0; i < pcAddr->allocated_chunks; i++) {
            if (pcAddr->chunknos[i]) {
                const char *formatStr = i == 0 ? "%u" : ",%u";
                errno_t rc =
                    snprintf_s(p, sizeof(buf) - (p - buf), sizeof(buf) - (p - buf) - 1, formatStr, pcAddr->chunknos[i]);
                securec_check_ss(rc, "\0", "\0");
                p += strlen(p);
            }
        }

        rc =
            memset_s((void *)pcAddr, SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size), 0, SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size));
        securec_check_c(rc, "\0", "\0");
        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                          errmsg("clean unused compress address of block %u in file \"%s\", old "
                                 "allocated_chunks/nchunks/chunknos: %u/%u/{%s}",
                                 blocknum, path, pcAddr->allocated_chunks, pcAddr->nchunks, buf)));
    }

    pfree(global_chunknos);

    if (pc_msync(pcMap) != 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not msync file \"%s\": %m", path)));
    }

    pcMap->last_recovery_start_time = pgStartTime;
}

int64 CalculateMainForkSize(char* pathName, RelFileNode* rnode, ForkNumber forkNumber)
{
    Assert(IS_COMPRESSED_RNODE((*rnode), forkNumber));
    Assert(rnode->bucketNode == -1);
    return CalculateCompressMainForkSize(pathName);
}

void CopyCompressedPath(char dst[MAXPGPATH], const char* pathName, CompressedFileType compressFileType)
{
    int rc;
    if (compressFileType == COMPRESSED_TABLE_PCA_FILE) {
        rc = snprintf_s(dst, MAXPGPATH, MAXPGPATH - 1, PCA_SUFFIX, pathName);
    } else {
        rc = snprintf_s(dst, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, pathName);
    }
    securec_check_ss(rc, "\0", "\0");
}

int64 CalculateCompressMainForkSize(char* pathName, bool suppressedENOENT)
{
    int64 totalsize = 0;

    char pcFilePath[MAXPGPATH];
    CopyCompressedPath(pcFilePath, pathName, COMPRESSED_TABLE_PCA_FILE);
    totalsize += CalculateFileSize(pcFilePath, MAXPGPATH, suppressedENOENT);

    CopyCompressedPath(pcFilePath, pathName, COMPRESSED_TABLE_PCD_FILE);
    totalsize += CalculateFileSize(pcFilePath, MAXPGPATH, suppressedENOENT);

    return totalsize;
}

uint16 ReadChunkSize(FILE* pcaFile, char* pcaFilePath, size_t len)
{
    uint16 chunkSize;
    if (fseeko(pcaFile, (off_t)offsetof(PageCompressHeader, chunk_size), SEEK_SET) != 0) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not seek in file \"%s\": \"%lu\": %m", pcaFilePath, len)));
    }

    if (fread(&chunkSize, sizeof(chunkSize), 1, pcaFile) <= 0) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not open file \"%s\": \"%lu\": %m", pcaFilePath, len)));
    }
    return chunkSize;
}

int64 CalculateFileSize(char* pathName, size_t size, bool suppressedENOENT)
{
    struct stat structstat;
    if (stat(pathName, &structstat)) {
        if (errno == ENOENT) {
            if (suppressedENOENT) {
                return 0;
            }
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not FIND file \"%s\": %m", pathName)));
        } else {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathName)));
        }
    }
    return structstat.st_size;
}

uint1 ConvertChunkSize(uint32 compressedChunkSize, bool *success)
{
    uint1 chunkSize = INDEX_OF_HALF_BLCKSZ;
    switch (compressedChunkSize) {
        case BLCKSZ / 2:
            chunkSize = INDEX_OF_HALF_BLCKSZ;
            break;
        case BLCKSZ / 4:
            chunkSize = INDEX_OF_QUARTER_BLCKSZ;
            break;
        case BLCKSZ / 8:
            chunkSize = INDEX_OF_EIGHTH_BRICK_BLCKSZ;
            break;
        case BLCKSZ / 16:
            chunkSize = INDEX_OF_SIXTEENTHS_BLCKSZ;
            break;
        default:
            *success = false;
            return chunkSize;
    }
    *success = true;
    return chunkSize;
}

constexpr int MAX_RETRY_LIMIT = 60;
constexpr long RETRY_SLEEP_TIME = 1000000L;

size_t ReadAllChunkOfBlock(char *dst, size_t destLen, BlockNumber blockNumber, ReadBlockChunksStruct& rbStruct)
{
    PageCompressHeader* header = rbStruct.header;
    if (blockNumber >= header->nblocks) {
        ereport(ERROR,
                (ERRCODE_INVALID_PARAMETER_VALUE,
                        errmsg("blocknum \"%u\" exceeds max block number", blockNumber)));
    }
    const char* fileName = rbStruct.fileName;
    decltype(PageCompressHeader::chunk_size) chunkSize = header->chunk_size;
    decltype(ReadBlockChunksStruct::segmentNo) segmentNo = rbStruct.segmentNo;
    PageCompressAddr* currentAddr = GET_PAGE_COMPRESS_ADDR(header, chunkSize, blockNumber);

    size_t tryCount = 0;
    /* for empty chunks write */
    uint8 allocatedChunks;
    uint8 nchunks;
    do {
        allocatedChunks = currentAddr->allocated_chunks;
        nchunks = currentAddr->nchunks;
        for (uint8 i = 0; i < nchunks; ++i) {
            off_t seekPos = (off_t)OFFSET_OF_PAGE_COMPRESS_CHUNK(chunkSize, currentAddr->chunknos[i]);
            uint8 start = i;
            while (i < nchunks - 1 && currentAddr->chunknos[i + 1] == currentAddr->chunknos[i] + 1) {
                i++;
            }
            if (fseeko(rbStruct.fp, seekPos, SEEK_SET) != 0) {
                ReleaseMap(header, fileName);
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek in file \"%s\": %m", fileName)));
            }
            size_t readAmount = chunkSize * (i - start + 1);
            if (fread(dst + start * chunkSize, 1, readAmount, rbStruct.fp) != readAmount && ferror(rbStruct.fp)) {
                ReleaseMap(header, fileName);
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", fileName)));
            }
        }
        if (nchunks == 0) {
            break;
        }
        char *data = NULL;
        size_t dataLen;
        uint32 crc32;
        if (PageIs8BXidHeapVersion(dst)) {
            HeapPageCompressData *heapPageData = (HeapPageCompressData *)dst;
            data = heapPageData->data;
            dataLen = heapPageData->size;
            crc32 = heapPageData->crc32;
        } else {
            PageCompressData *heapPageData = (PageCompressData *)dst;
            data = heapPageData->data;
            dataLen = heapPageData->size;
            crc32 = heapPageData->crc32;
        }
        if (DataBlockChecksum(data, dataLen, true) == crc32) {
            break;
        }

        if (tryCount < MAX_RETRY_LIMIT) {
            ++tryCount;
            pg_usleep(RETRY_SLEEP_TIME);
        } else {
            ReleaseMap(header, fileName);
            ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("base backup cheksum or Decompressed blockno %u failed in file \"%s\", aborting backup. "
                           "nchunks: %u, allocatedChunks: %u, segno: %d.",
                        blockNumber,
                        fileName,
                        nchunks,
                        allocatedChunks,
                        segmentNo)));
        }
    } while (true);
    if (allocatedChunks > nchunks) {
        auto currentWriteSize = nchunks * chunkSize;
        securec_check(
            memset_s(dst + currentWriteSize, destLen - currentWriteSize, 0, (allocatedChunks - nchunks) * chunkSize),
            "",
            "");
    }
    return allocatedChunks * chunkSize;
}

void ReleaseMap(PageCompressHeader* map, const char* fileName)
{
    if (map != NULL && pc_munmap(map) != 0) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not munmap file \"%s\": %m", fileName)));
    }
}
