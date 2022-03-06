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
 * parquet_input_stream_adapter.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/parquet/parquet_input_stream_adapter.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PARQUET_INPUT_STREAM_ADAPTER_H
#define PARQUET_INPUT_STREAM_ADAPTER_H

#include "access/dfs/dfs_stream.h"
#include "access/dfs/dfs_am.h"
#ifndef ENABLE_LITE_MODE
#include "parquet/api/reader.h"
#endif
#include "utils/memutils.h"

namespace dfs {
namespace reader {
using Buffer = ::arrow::Buffer;

constexpr size_t MAX_ALLOC_SIZE = MaxAllocSize;

class ParquetInputStreamAdapter : public parquet::RandomAccessSource {
public:
    ParquetInputStreamAdapter(MemoryContext memoryContext, std::unique_ptr<dfs::GSInputStream> gsInputStream);
    ~ParquetInputStreamAdapter();

    int64_t Size() const override;

    int64_t Read(int64_t nBytes, uint8_t *out) override;
    std::shared_ptr<Buffer> Read(int64_t nBytes) override;

    std::shared_ptr<Buffer> ReadAt(int64_t position, int64_t nBytes) override;
    int64_t ReadAt(int64_t position, int64_t nBytes, uint8_t *out) override;

    void Close() override;
    int64_t Tell() override;

private:
    void allocateBuffer(int64_t nBytes);
    void releaseBuffer();
    void zeroPadding(int64_t nBytes) const;

    inline bool allocSizeIsValid(const int64_t nBytes) const
    {
        return (size_t)(nBytes) <= MAX_ALLOC_SIZE;
    }

    std::unique_ptr<dfs::GSInputStream> m_gsInputStream;
    MemoryContext m_internalContext;
    uint64_t m_offset;
    uint8_t *m_buffer;
    size_t m_bufferSize;
};
}  // namespace reader
}  // namespace dfs

#endif
