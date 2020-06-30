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
 * transaction_buffer_iterator.h
 *    Iterator for iterating over redo log transactions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/recovery/transaction_buffer_iterator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef TRANSACTION_BUFFER_ITERATOR_H
#define TRANSACTION_BUFFER_ITERATOR_H

#include "redo_log_global.h"
#include "redo_log_writer.h"
#include "serializable.h"

namespace MOT {
/**
 * @struct LogSegment
 * @brief encapsulates a chunk of logging data
 */
struct LogSegment : public Serializable {
    char* m_data;

    size_t m_len;

    EndSegmentBlock m_controlBlock;

    /**
     * @brief fetches the size of the log segment
     * @return Size_t value denoting the size of the segment.
     */
    virtual size_t SerializeSize();

    /**
     * @brief serialize the log segment into a given buffer
     * @param dataOut the output buffer
     */
    virtual void Serialize(char* dataOut);

    /**
     * @brief creates a log segment from a data buffer.
     * @param dataIn the input buffer.
     */
    virtual void Deserialize(const char* dataIn);
};

/**
 * @class RedoLogBufferIterator
 * @brief Iterator for iterating over redo log transactions.
 */
class RedoLogTransactionIterator {
public:
    RedoLogTransactionIterator(char* data, uint32_t length) : m_buffer(data), m_bufferLength(length), m_position(0)
    {
        m_txnLength = *(reinterpret_cast<uint32_t*>(m_buffer));
        m_endSegment = *(reinterpret_cast<EndSegmentBlock*>(m_buffer + m_txnLength - sizeof(EndSegmentBlock)));
    }

    ~RedoLogTransactionIterator()
    {}

    bool Next();

    bool End();

    inline uint64_t GetExternalTransactionId() const
    {
        return m_endSegment.m_externalTransactionId;
    }

    inline uint64_t GetInternalTransactionId() const
    {
        return m_endSegment.m_internalTransactionId;
    }

    inline uint64_t GetCsn() const
    {
        return m_endSegment.m_csn;
    }

    inline OperationCode GetOperationCode() const
    {
        return m_endSegment.m_opCode;
    }

    inline uint32_t GetRedoTransactionLength() const
    {
        return m_txnLength;
    }

    void* GetTransactionEntry();

    LogSegment* AllocRedoSegment();

private:
    char* m_buffer;

    uint32_t m_bufferLength;

    size_t m_position;

    uint32_t m_txnLength;

    EndSegmentBlock m_endSegment;
};
}  // namespace MOT

#endif /* TRANSACTION_BUFFER_ITERATOR_H */
