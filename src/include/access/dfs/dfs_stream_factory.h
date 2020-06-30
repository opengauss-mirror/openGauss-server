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
 * ---------------------------------------------------------------------------------------
 *
 * dfs_stream_factory.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/dfs/dfs_stream_factory.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DFS_STREAM_FACTORY_H
#define DFS_STREAM_FACTORY_H

#include <string>

#include "access/dfs/dfs_am.h"
#include "access/dfs/dfs_stream.h"

namespace dfs {

DFS_UNIQUE_PTR<dfs::GSInputStream> InputStreamFactory(dfs::DFSConnector *conn, const std::string &path,
                                                      dfs::reader::ReaderState *readerState, bool use_cache);
}
#endif
