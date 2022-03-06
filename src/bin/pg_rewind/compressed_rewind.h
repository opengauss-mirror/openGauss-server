/* -------------------------------------------------------------------------
 *
 * compressed_rewind.h
 *
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * -------------------------------------------------------------------------
 */
#ifndef OPENGAUSS_SERVER_COMPRESS_COMPRESSED_REWIND_H
#define OPENGAUSS_SERVER_COMPRESS_COMPRESSED_REWIND_H

#include "compressed_common.h"
#include "storage/page_compression.h"
#include "storage/smgr/relfilenode.h"

extern bool FetchSourcePca(const char* strValue, RewindCompressInfo* rewindCompressInfo);
extern bool ProcessLocalPca(const char* tablePath, RewindCompressInfo* rewindCompressInfo);
extern void FormatPathToPca(const char* path, char* dst, size_t len, bool withPrefix = false);
extern void FormatPathToPcd(const char* path, char* dst, size_t len, bool withPrefix = false);

#endif  // OPENGAUSS_SERVER_COMPRESS_COMPRESSED_REWIND_H
