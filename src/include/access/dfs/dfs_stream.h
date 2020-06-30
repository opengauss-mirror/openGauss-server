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
 * dfs_stream.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/dfs/dfs_stream.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DFS_STREAM_H
#define DFS_STREAM_H

#include <string>
#include <sstream>
#include <memory>

#include "dfs_config.h"  // for DFS_UNIQUE_PTR

namespace dfs {

class GSInputStream {
public:
    virtual ~GSInputStream(){};

    /*
     * Get the total length of the file in bytes.
     */
    virtual uint64_t getLength() const = 0;

    /*
     * Get the natural size for reads.
     * @return the number of bytes that should be read at once
     */
    virtual uint64_t getNaturalReadSize() const = 0;

    /*
     * Read length bytes from the file starting at offset into
     * the buffer starting at buf.
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to read.
     * @param offset the position in the stream to read from.
     */
    virtual void read(void *buf, uint64_t length, uint64_t offset) = 0;

    /*
     * Get the name of the stream for error messages.
     */
    virtual const std::string &getName() const = 0;

    virtual void getStat(uint64_t *localBlock, uint64_t *remoteBlock, uint64_t *nnCalls, uint64_t *dnCalls) = 0;

    virtual void getLocalRemoteReadCnt(uint64_t *localReadCnt, uint64_t *remoteReadCnt) = 0;

    virtual DFS_UNIQUE_PTR<GSInputStream> copy() = 0;
};

}  // namespace dfs

#endif
