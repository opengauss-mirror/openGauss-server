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
 * occ_transaction_manager.h
 *    Optimistic Concurrency Control (OCC) implementation
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/concurrency_control/occ_transaction_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef OCC_TRANSACTION_MANAGER_H
#define OCC_TRANSACTION_MANAGER_H

#include "global.h"

namespace MOT {
// forward declaration
class Access;
class TxnManager;

constexpr uint64_t LOCK_TIME_OUT = 1 << 16;
/**
 * @class OccTransactionManager
 * @brief Optimistic concurrency control implementation.
 */
class OccTransactionManager {
public:
    /** @brief Constructor. */
    OccTransactionManager();

    /** @brief Destructor. */
    ~OccTransactionManager();

    /**
     * @brief Initialize _writeSet, _readSet and _insertSet array.
     */
    bool Init();

    /**
     * @brief Sets or clears the pre-abort flag.
     * @detail Determines whether to call quickVersionCheck() during
     * transaction commit-validation to detect quickly invalid access items
     * that cause transaction abort.
     * @param b The new pre-abort flag state.
     */
    void SetPreAbort(bool b)
    {
        m_preAbort = b;
    }

    /**
     * @brief Sets or clears the validate-no-wait flag.
     * @detail Determines whether to call Access::lock() or
     * Access::try_lock() on each access item in the write set.
     * @param b The new validate-no-wait flag state.
     */
    void SetValidationNoWait(bool b)
    {
        m_validationNoWait = b;
    }

    /**
     * @brief Performs OCC validation for a transaction commit.
     * @param tx The committed transaction.
     * @return Return code denoting whether transaction committed
     * successfully or not.
     */
    RC ValidateOcc(TxnManager* tx);

    RC LockHeaders(TxnManager* txMan, uint32_t& numSentinelsLock);

    RC LockRows(TxnManager* txMan, uint32_t& numRowsLock);

    /**
     * @brief Writes all the changes in the write set of a transaction and
     * release the locks associated with all the write access items.
     * @param txMan The committing transaction.
     */
    void WriteChanges(TxnManager* txMan);

    /** @brief remove all deleted keys from the global indices   */
    void CleanRowsFromIndexes(TxnManager* txMan);

    /** @brief Rollack insert-set due to an abort   */
    void RollbackInserts(TxnManager* txMan);

    void ReleaseLocks(TxnManager* txMan)
    {
        if (m_rowsLocked) {
            ReleaseHeaderLocks(txMan, m_writeSetSize);
            ReleaseRowsLocks(txMan, m_rowsSetSize);
            m_rowsLocked = false;
        }
    }

    void ReleaseHeaders(TxnManager* txMan)
    {
        if (m_rowsLocked) {
            ReleaseHeaderLocks(txMan, m_writeSetSize);
            m_rowsLocked = false;
        }
    }

    /**
     * @brief Clean Up OCC Transaction state.
     */
    void CleanUp();

    /**
     * @brief Generate an estimation to decide whether to sleep or spin
     * @return result true = high contention
     */
    inline bool IsHighContention()
    {
        double ratio = (double)m_abortsCounter / (double(m_txnCounter));
        // Over 30% aborts is the baseline for high-contention
        if (ratio > 0.3) {
            m_dynamicSleep = 100;
            if (ratio > 0.5) {
                m_dynamicSleep = 500;
            }
            return true;
        } else {
            return false;
        }
    }

private:
    /**
     * @brief Checks whether the transaction identifier in the original row
     * in the access matches the transaction identifier in the access object.
     * @detail The check is faster than a cc validate, but it does not
     * guarantee correctness. If the versions do not match, cc
     * verification will always fail too. However, if the function returns
     * true, cc may fail during the verification (i.e. it may produce
     * false positive reports).
     */
    bool CheckVersion(const Access* access);

    /** @brief Validate Header for insert */
    bool QuickHeaderValidation(const Access* access);

    bool QuickVersionCheck(TxnManager* txMan, uint32_t& readSetSize);

    bool LockHeadersNoWait(TxnManager* txMan, uint32_t& numSentinelsLock);

    void ReleaseHeaderLocks(TxnManager* txMan, uint32_t numOfLocks);

    /** @brief Release all the locked rows */
    void ReleaseRowsLocks(TxnManager* txMan, uint32_t numOfLocks);

    /** @brief Validate the read set */
    bool ValidateReadSet(TxnManager* txMan);

    /** @brief Validate the write set */
    bool ValidateWriteSet(TxnManager* txMan);

    /** @brief Pre-allocates stable row according to the checkpoint state. */
    bool PreAllocStableRow(TxnManager* txMan);

    /** @brief Sets stable row according to the checkpoint state. */
    void ApplyWrite(TxnManager* txMan);

    /** @var transaction counter   */
    uint32_t m_txnCounter;

    /** @var aborts counter   */
    uint32_t m_abortsCounter;

    /** @var Write set size. */
    uint32_t m_writeSetSize;

    /** @var total number of rows   */
    uint32_t m_rowsSetSize;

    /** @var Write set size. */
    uint32_t m_deleteSetSize;

    /** @var Write set size. */
    uint32_t m_insertSetSize;

    uint16_t m_dynamicSleep;

    /** @var flag indicating whether we locked the rows   */
    bool m_rowsLocked;

    /** @var Pre-abort configuration. */
    bool m_preAbort;

    /** @var Validate-no-wait configuration. */
    bool m_validationNoWait;
};
}  // namespace MOT

#endif /* OCC_TRANSACTION_MANAGER_H */
