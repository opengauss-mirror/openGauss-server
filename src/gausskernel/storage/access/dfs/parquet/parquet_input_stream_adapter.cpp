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
 * parquet_input_stream_adapter.cpp
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/parquet/parquet_input_stream_adapter.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifndef ENABLE_LITE_MODE
#include "parquet/platform.h"
#include "parquet/types.h"
#endif
#include "parquet_input_stream_adapter.h"
#include "postgres.h"
#include "knl/knl_variable.h"

namespace dfs {
namespace reader {
ParquetInputStreamAdapter::ParquetInputStreamAdapter(MemoryContext memoryContext,
                                                     std::unique_ptr<dfs::GSInputStream> gsInputStream)
    : m_gsInputStream(std::move(gsInputStream)),
      m_internalContext(memoryContext),
      m_offset(0),
      m_buffer(NULL),
      m_bufferSize(0)
{
}

ParquetInputStreamAdapter::~ParquetInputStreamAdapter()
{
    m_buffer = NULL;
    m_internalContext = NULL;
}

int64_t ParquetInputStreamAdapter::Size() const
{
    return static_cast<int64_t>(m_gsInputStream->getLength());
}

int64_t ParquetInputStreamAdapter::Read(int64_t nBytes, uint8_t *out)
{
    m_gsInputStream->read(out, nBytes, m_offset);
    m_offset += nBytes;
    return nBytes;
}

std::shared_ptr<Buffer> ParquetInputStreamAdapter::Read(int64_t nBytes)
{
    allocateBuffer(nBytes);
    int64_t nBytesRead = Read(nBytes, m_buffer);
    zeroPadding(nBytesRead);
    return arrow::Buffer::Wrap(m_buffer, nBytesRead);
}

std::shared_ptr<Buffer> ParquetInputStreamAdapter::ReadAt(int64_t position, int64_t nBytes)
{
    allocateBuffer(nBytes);
    int64_t nBytesRead = ReadAt(position, nBytes, m_buffer);
    zeroPadding(nBytesRead);
    return arrow::Buffer::Wrap(m_buffer, nBytesRead);
}

int64_t ParquetInputStreamAdapter::ReadAt(int64_t position, int64_t nBytes, uint8_t *out)
{
    m_gsInputStream->read(out, nBytes, position);
    m_offset = (uint64_t)(position + nBytes);
    return nBytes;
}

void ParquetInputStreamAdapter::Close()
{
}

int64_t ParquetInputStreamAdapter::Tell()
{
    return (int64_t)(m_offset);
}

void ParquetInputStreamAdapter::allocateBuffer(int64_t nBytes)
{
    if (m_buffer != NULL) {
        if (static_cast<int64_t>(m_bufferSize) >= nBytes) {
            return;
        }

        releaseBuffer();
        m_buffer = NULL;
    }

    if (allocSizeIsValid(nBytes)) {
        AutoContextSwitch newMemCnxt(m_internalContext);
        m_buffer = (uint8_t *)palloc0(nBytes);
    } else {
        m_buffer = (uint8_t *)palloc_huge(m_internalContext, nBytes);
        errno_t rc = memset_s(m_buffer, (size_t)(nBytes), 0, (size_t)(nBytes));
        securec_check(rc, "\0", "\0");
    }

    m_bufferSize = (size_t)(nBytes);
}

void ParquetInputStreamAdapter::releaseBuffer()
{
    if (m_buffer == NULL) {
        return;
    }

    pfree(m_buffer);
    m_buffer = NULL;
    m_bufferSize = 0;
}

void ParquetInputStreamAdapter::zeroPadding(const int64_t nBytes) const
{
    if (nBytes >= static_cast<int64_t>(m_bufferSize)) {
        return;
    }
    auto len = static_cast<size_t>(m_bufferSize - nBytes);
    errno_t rc = memset_s(m_buffer + nBytes, len, 0, len);
    securec_check(rc, "\0", "\0");
}
}  // namespace reader
}  // namespace dfs
