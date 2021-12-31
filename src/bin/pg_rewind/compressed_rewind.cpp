/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
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
 * compressed_rewind.cpp
 *	  Functions for fetching compressed table.
 *
 *
 * IDENTIFICATION
 *    ./src/bin/pg_rewind/compressed_rewind.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "compressed_rewind.h"
#include "libpq/libpq-fe.h"
#include "lib/string.h"
#include "logging.h"
#include "filemap.h"
#include "utils/elog.h"
#include "file_ops.h"

void FormatPathToPca(const char* path, char* dst, size_t len, bool withPrefix)
{
    errno_t rc;
    if (withPrefix) {
        rc = snprintf_s(dst, len, len - 1, "%s/" PCA_SUFFIX, pg_data, path);
    } else {
        rc = snprintf_s(dst, len, len - 1, PCA_SUFFIX, path);
    }
    securec_check_ss_c(rc, "\0", "\0");
}

void FormatPathToPcd(const char* path, char* dst, size_t len, bool withPrefix)
{
    errno_t rc;
    if (withPrefix) {
        rc = snprintf_s(dst, len, len - 1, "%s/" PCD_SUFFIX, pg_data, path);
    } else {
        rc = snprintf_s(dst, len, len - 1, PCD_SUFFIX, path);
    }
    securec_check_ss_c(rc, "\0", "\0");
}

template <typename T>
bool ReadCompressedInfo(T& t, off_t offset, FILE* file, char* pcaFilePath, size_t len)
{
    if (fseeko(file, offset, SEEK_SET) != 0) {
        pg_fatal("could not seek in file \"%s\": \"%lu\": %s\n", pcaFilePath, len, strerror(errno));
        return false;
    }
    if (fread(&t, sizeof(t), 1, file) <= 0) {
        pg_fatal("could not open file \"%s\": \"%lu\": %s\n", pcaFilePath, len, strerror(errno));
        return false;
    }
    return true;
}

/**
 * write RewindCompressInfo
 * @param file file fp
 * @param pcaFilePath file path,for ereport
 * @param rewindCompressInfo pointer of return
 * @return sucesss or not
 */
static bool ReadRewindCompressedInfo(FILE* file, char* pcaFilePath, size_t len, RewindCompressInfo* rewindCompressInfo)
{
    off_t offset = (off_t)offsetof(PageCompressHeader, chunk_size);
    if (!ReadCompressedInfo(rewindCompressInfo->chunkSize, offset, file, pcaFilePath, len)) {
        return false;
    }
    offset = (off_t)offsetof(PageCompressHeader, algorithm);
    if (!ReadCompressedInfo(rewindCompressInfo->algorithm, offset, file, pcaFilePath, len)) {
        return false;
    }
    offset = (off_t)offsetof(PageCompressHeader, nblocks);
    if (!ReadCompressedInfo(rewindCompressInfo->oldBlockNumber, offset, file, pcaFilePath, len)) {
        return false;
    }
    rewindCompressInfo->compressed = true;
    return true;
}

bool FetchSourcePca(const char* strValue, RewindCompressInfo* rewindCompressInfo)
{
    size_t length = 0;
    PageCompressHeader* ptr = (PageCompressHeader*)PQunescapeBytea((const unsigned char*)strValue, &length);
    rewindCompressInfo->compressed = false;
    if (length == sizeof(PageCompressHeader)) {
        rewindCompressInfo->compressed = true;
        rewindCompressInfo->algorithm = ptr->algorithm;
        rewindCompressInfo->newBlockNumber = ptr->nblocks;
        rewindCompressInfo->oldBlockNumber = 0;
        rewindCompressInfo->chunkSize = ptr->chunk_size;
    }
    PQfreemem(ptr);
    return rewindCompressInfo->compressed;
}

bool ProcessLocalPca(const char* tablePath, RewindCompressInfo* rewindCompressInfo)
{
    rewindCompressInfo->compressed = false;
    if (!isRelDataFile(tablePath)) {
        return false;
    }
    char pcaFilePath[MAXPGPATH];
    FormatPathToPca(tablePath, pcaFilePath, MAXPGPATH, true);
    FILE* file = fopen(pcaFilePath, "rb");
    if (file == NULL) {
        if (errno == ENOENT) {
            return false;
        }
        pg_fatal("could not open file \"%s\": %s\n", pcaFilePath, strerror(errno));
        return false;
    }
    bool success = ReadRewindCompressedInfo(file, pcaFilePath, MAXPGPATH, rewindCompressInfo);
    fclose(file);
    return success;
}
