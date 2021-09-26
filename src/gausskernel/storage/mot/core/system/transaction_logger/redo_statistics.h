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
 * redo_statistics.h
 *    Manages global redo log statistics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_STATISTICS_H
#define REDO_STATISTICS_H

#include <array>
#include <string>

#include "redo_log_global.h"

namespace MOT {
/**
 * @class redo_statistics
 * @brief Manages global redo log statistics.
 */
class RedoStatistics {
public:
    /** @brief Constructor. */
    RedoStatistics();

    /**
     * @brief Records a processing of a single operation.
     * @param op_code The operation code.
     * @param size The operation size.
     */
    void OperationProcessed(OperationCode opCode, uint32_t size);

    /**
     * @brief Converts statistics to formatted string.
     * @return The statistics string.
     */
    std::string ToString() const;

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    RedoStatistics(const RedoStatistics& orig) = delete;
    RedoStatistics(const RedoStatistics&& orig) = delete;
    RedoStatistics& operator=(const RedoStatistics& orig) = delete;
    RedoStatistics& operator=(const RedoStatistics&& orig) = delete;
    /** @endcond */

private:
    /** @var Maximum number of operations. */
    static constexpr uint32_t NUM_OPERATIONS = uint32_t(OperationCode::INVALID_OPERATION_CODE);

    /** @var Operations per code statistics. */
    std::array<uint64_t, NUM_OPERATIONS> m_operationsPerCode;

    /** @var Total consumed bytes. */
    uint64_t m_consumedBytes;
};
}  // namespace MOT

#endif /* REDO_STATISTICS_H */
