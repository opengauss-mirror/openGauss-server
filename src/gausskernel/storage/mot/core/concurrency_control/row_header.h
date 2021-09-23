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
 * row_header.h
 *    Row header implementation in OCC
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/concurrency_control/row_header.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ROW_HEADER_H
#define ROW_HEADER_H

#include <cstdint>
#include "global.h"
#include "mot_atomic_ops.h"
#include "debug_utils.h"

namespace MOT {
// forward declarations
class Catalog;
class Index;
class Row;
class TxnManager;
class TxnAccess;
class CheckpointWorkerPool;
class RecoveryOps;
// forward declarations
enum RC : uint32_t;
enum AccessType : uint8_t;

// forward declaration
class OccTransactionManager;
class Access;

/** @define masks for CSN word   */
#define CSN_BITS 0x1FFFFFFFFFFFFFFFUL

#define STATUS_BITS 0xE000000000000000UL

/** @define Bit position designating locked state. */
#define LOCK_BIT (1UL << 63)

/** @define Bit position designation version. */
#define LATEST_VER_BIT (1UL << 62)

/** @define Bit position designating absent row. */
#define ABSENT_BIT (1UL << 61)

/**
 * @class RowHeader
 * @brief Helper class for managing a single row in Optimistic concurrency control.
 */
class __attribute__((__packed__)) RowHeader {
public:
    /**
     * @brief Constructor.
     * @param row The row manage.
     */
    explicit inline RowHeader() : m_csnWord(0)
    {}

    inline RowHeader(const RowHeader& src)
    {
        m_csnWord = src.m_csnWord;
    }

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    RowHeader(RowHeader&&) = delete;

    RowHeader& operator=(const RowHeader&) = delete;

    RowHeader& operator=(RowHeader&&) = delete;
    /** @endcond */

private:
    /**
     * @brief Gets a consistent snapshot of a managed row.
     * @param txn The transaction access object.
     * @param type The access type.
     * @param[out] localRow Receives the row contents except during new row
     * insertion.
     * @return Return code denoting success or failure.
     */
    RC GetLocalCopy(TxnAccess* txn, AccessType type, Row* localRow, const Row* origRow, TransactionId& lastTid) const;

    /**
     * @brief Validates the row was not changed by a concurrent transaction
     * @param tid The transaction identifier.
     * @return Boolean value denoting whether the row header is valid.
     */
    bool ValidateWrite(TransactionId tid) const;

    /**
     * @brief Validates the row was not changed or locked by a concurrent transaction
     * @param tid The transaction identifier.
     * @return Boolean value denoting whether the row header is valid.
     */
    bool ValidateRead(TransactionId tid) const;

    /**
     * @brief Apply changes to the public row
     * @param access Container for the row
     * @param csn Commit Serial Number
     */
    void WriteChangesToRow(const Access* access, uint64_t csn);

    /** @brief Locks the row. */
    void Lock();

    /** @brief Unlocks the row. */
    void Release()
    {
        MOT_ASSERT(m_csnWord & LOCK_BIT);
#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        m_csnWord = m_csnWord & (~LOCK_BIT);
#else
        uint64_t v = m_csnWord;
        while (!__sync_bool_compare_and_swap(&m_csnWord, v, (v & ~LOCK_BIT))) {
            PAUSE
            v = m_csnWord;
        }
#endif
    }

    /**
     * @brief Attempts to lock the row.
     * @return True if the row was locked.
     */
    bool TryLock()
    {
        uint64_t v = m_csnWord;
        if (v & LOCK_BIT) {  // already locked
            return false;
        }
        return __sync_bool_compare_and_swap(&m_csnWord, v, (v | LOCK_BIT));
    }

    /** @var Assert row is locked. */
    void AssertLock() const
    {
        MOT_ASSERT(m_csnWord & LOCK_BIT);
    }

    /**
     * @brief Retrieves the transaction identifier stripped off all
     * meta-data.
     * @return The bare transaction identifier.
     */
    uint64_t GetCSN() const
    {
        return (m_csnWord & CSN_BITS);
    }

    /** Set the CSN of the row */
    void SetCSN(uint64_t csn)
    {
        m_csnWord = (m_csnWord & STATUS_BITS) | (csn & CSN_BITS);
    }

    /**
     * @brief Queries whether the absent bit is set for the row.
     * @return True if the absent bit is set in the row.
     */
    bool IsAbsent() const
    {
        return (m_csnWord & ABSENT_BIT) == ABSENT_BIT;
    }

    /**
     * @brief Queries whether the lock bit is set in the row.
     * @return True if the lock bit is set in the row.
     */
    bool IsLocked() const
    {
        return (m_csnWord & LOCK_BIT) == LOCK_BIT;
    }

    /**
     * @brief Queries whether the row is deleted.
     * @return True if the absent bit and the latet vsersion are set for the row.
     */
    bool IsRowDeleted() const
    {
        return (m_csnWord & (ABSENT_BIT | LATEST_VER_BIT)) == (ABSENT_BIT | LATEST_VER_BIT);
    }

    /** @brief Sets the absent bit in the row. */
    void SetAbsentBit()
    {
        m_csnWord |= ABSENT_BIT;
    }

    /** @brief Sets the absent and locked bits in the row. */
    void SetAbsentLockedBit()
    {
        m_csnWord |= (ABSENT_BIT | LOCK_BIT);
    }

    /** @brief Set the deletion bits in the row  */
    void SetDeleted()
    {
        m_csnWord |= (ABSENT_BIT | LATEST_VER_BIT);
    }

    /** @brief Clears the absent bit in the row. */
    void UnsetAbsentBit()
    {
        m_csnWord &= ~ABSENT_BIT;
    }

    /** @brief Usnet latest version bit   */
    void UnsetLatestVersionBit()
    {
        m_csnWord &= ~LATEST_VER_BIT;
    }

private:
    /** @var Transaction identifier and meta data for locking and deleting a row. */
    volatile uint64_t m_csnWord;

    // Allows privileged access
    friend Row;
    friend OccTransactionManager;
    friend CheckpointWorkerPool;
    friend RecoveryOps;
    friend Index;
};
}  // namespace MOT

#endif /* ROW_HEADER_H */
