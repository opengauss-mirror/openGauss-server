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
 * statistic_variable.h
 *    The base class for all statistic variable types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STATISTIC_VARIABLE_H
#define STATISTIC_VARIABLE_H

#include "global.h"
#include "utilities.h"

/** @define Maximum size of statistic variable name. */
constexpr size_t STAT_VAR_MAX_NAME_LEN = 64;

namespace MOT {
/**
 * @class StatisticVariable
 * @brief The base class for all statistic variable types.
 */
class StatisticVariable {
public:
    /**
     * @brief Constructor.
     * @param name Name used for printing.
     */
    explicit StatisticVariable(const char* name) : m_count(0)
    {
        SetName(name);
    }

    /** @brief Destructor. */
    virtual ~StatisticVariable()
    {}

    /** @brief Retrieves the name of the statistic variable. */
    inline const char* GetName() const
    {
        return m_name;
    }

    /**
     * @brief Sets the statistics variable name.
     */
    inline void SetName(const char* name)
    {
        errno_t erc = snprintf_s(m_name, STAT_VAR_MAX_NAME_LEN, STAT_VAR_MAX_NAME_LEN - 1, "%s", name);
        securec_check_ss(erc, "\0", "\0");
    }

    /**
     * @brief Summarizes statistics for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    virtual void Summarize(bool updateTstamp) = 0;

    /**
     * @brief Prints statistics to log file using the given log level.
     * @param log_levelThe log level to use.
     */
    virtual void Print(LogLevel logLevel) const = 0;

    /**
     * @brief Copies statistics from another object into this object.
     * @param rhs The right-hand-side object to copy from.
     */
    virtual void Assign(const StatisticVariable& rhs) = 0;

    /**
     * @brief Adds statistics of another object to this object.
     * @param rhs The right-hand-side object to add from.
     */
    virtual void Add(const StatisticVariable& rhs) = 0;

    /**
     * @brief Subtracts statistics of another object from this object.
     * @param rhs The right-hand-side subtrahend object.
     */
    virtual void Subtract(const StatisticVariable& rhs) = 0;

    /**
     * @brief Divides the statistics of this object by a given factor.
     * @param factor The division factor.
     */
    virtual void Divide(uint32_t factor) = 0;

    /**
     * @brief Resets all statistic values to zero.
     */
    virtual void Reset() = 0;

    /**
     * Retrieves the amount of samples accumulated in this statistic variable.
     * @return The sample count.
     */
    inline uint64_t GetSampleCount() const
    {
        return m_count;
    }

protected:
    /** @var Name used for printing. */
    char m_name[STAT_VAR_MAX_NAME_LEN];

    /** @var Sample count. */
    uint64_t m_count __attribute__((aligned(64)));

    /** @var Make sure noisy counter lives in its own cache line. */
    uint8_t m_padding[64 - sizeof(m_count)];

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT
#endif /* STATISTIC_VARIABLE_H */
