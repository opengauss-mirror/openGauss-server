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

int64 CalculateMainForkSize(char* pathName, RelFileNode* rnode, ForkNumber forkNumber)
{
    Assert(IS_COMPRESSED_RNODE((*rnode), forkNumber));
    Assert(rnode->bucketNode == -1);
    return CalculateCompressMainForkSize(pathName);
}

void CopyCompressedPath(char *dst, const char* pathName)
{
    int rc = snprintf_s(dst, MAXPGPATH, MAXPGPATH - 1, COMPRESS_SUFFIX, pathName);
    securec_check_ss(rc, "\0", "\0");
}

int64 CalculateCompressMainForkSize(char* pathName, bool suppressedENOENT)
{
    int64 totalsize = 0;
    totalsize += CalculateFilePhyRealSize(pathName, suppressedENOENT);
    return totalsize;
}

int64 CalculateFileSize(char* pathName, bool suppressedENOENT)
{
    struct stat structstat;
    if (stat(pathName, &structstat)) {
        if (errno == ENOENT) {
            if (suppressedENOENT)
                return 0;
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not FIND file \"%s\": ", pathName)));
        } else
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": ", pathName)));
    }
    return structstat.st_size;
}

int64 CalculateFilePhyRealSize(char* pathName, bool suppressedENOENT)
{
    struct stat structstat;
    if (stat(pathName, &structstat)) {
        if (errno == ENOENT) {
            if (suppressedENOENT) {
                return 0;
            }
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not FIND file \"%s\": ", pathName)));
        } else {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": ", pathName)));
        }
    }
    return structstat.st_blocks * FILE_BLOCK_SIZE_512;
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

#ifndef FRONTEND

void CompressTimerStart(std::chrono::high_resolution_clock::time_point &compressStart)
{
    compressStart = std::chrono::high_resolution_clock::now();
}

void CompressTimerEnd(std::chrono::high_resolution_clock::time_point compressStart,
                      RelFileCompressOption option)
{
    auto compressEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<long long, std::nano> compress_time =
        std::chrono::duration_cast<std::chrono::duration<long long, std::nano>>(
            compressEnd - compressStart);
    t_thrd.page_compression_cxt.compressPageCnt++;
    t_thrd.page_compression_cxt.compressNanoSeconds += compress_time.count();

    if (t_thrd.page_compression_cxt.compressPageCnt % COMPRESS_PAGE_COUNT_THRESHOLD == 0) {
        ereport(LOG, (errmsg(
            "Compress status: compress page count %lu, avg compression time %lu nano seconds. %s",
            t_thrd.page_compression_cxt.compressPageCnt,
            t_thrd.page_compression_cxt.compressNanoSeconds /
            t_thrd.page_compression_cxt.compressPageCnt,
            option.toString().c_str())));
    }
}

const ZSTD_parameters g_zstd_params = {
    {14,         // windowLog, largest match distance, default 14.
     14,         // chainLog, fully searched segment, default 14.
     7,          // hashLog, dispatch table, default 15.
     1,          // searchLog, nb of searches, default 1.
     7,          // minMatch, match length searched, default 5.
     0,          // targetLength, acceptable match size for optimal parser, default 0.
     ZSTD_fast}, // strategy, see ZSTD_strategy definition, default ZSTD_fast.
    {0,          // contentSizeFlag, 1:content size will be in frame header, default 0.
     0,          // checksumFlag, 1:generate a 32-bits checksum, default 0.
     0}          // noDictIDFlag, 1:no dictID will be saved into frame header, default 0.
};

#endif
