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
 * OrcObsFile.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/orc/OrcObsFile.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ORCOBSFILE_H
#define ORCOBSFILE_H

#include <string>

#include "access/dfs/dfs_am.h"
#include "access/dfs/dfs_stream.h"
#include "access/obs/obs_am.h"

namespace dfs {
/**
 * Create a OBS file input stream for the given path.
 * @param fs: the OBS connector handler.
 * @param path: the absolute file path.
 * @param readerState: the state of reading which includes all the state variables.
 * @return the input stream
 */
DFS_UNIQUE_PTR<GSInputStream> readObsFile(OBSReadWriteHandler *handler, const std::string &path,
                                          dfs::reader::ReaderState *readerState);

/**
 * Create a OBS file input stream for the given path with cache.
 * @param fs: the OBS connector handler.
 * @param path: the absolute file path.
 * @param readerState: the state of reading which includes all the state variables.
 * @return the input stream
 */
DFS_UNIQUE_PTR<GSInputStream> readObsFileWithCache(OBSReadWriteHandler *handler, const std::string &path,
                                                   dfs::reader::ReaderState *readerState);
}  // namespace dfs
#endif
