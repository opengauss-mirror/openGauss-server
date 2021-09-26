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
 * boolean_statistic_variable.h
 *    A statistic variable used to collect statistics for a boolean value.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/boolean_statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BOOLEAN_STATISTIC_VARIABLE_H
#define BOOLEAN_STATISTIC_VARIABLE_H

#include "statistic_variable.h"

namespace MOT {
/**
 * @class BooleanStatisticVariable
 * @brief A statistic variable used to collect statistics for a boolean value.
 * In particular the occurrence percentage of each value is computed.
 */
class BooleanStatisticVariable : public StatisticVariable {
public:
    /**
     * @brief Constructor.
     * @param name Name used for printing.
     */
    explicit BooleanStatisticVariable(const char* name)
        : StatisticVariable(name), m_sum(0), m_percentage(0.0f), m_countSaved(0)
    {}

    /** @brief Destructor. */
    ~BooleanStatisticVariable() final
    {}

    /**
     * @brief Summarizes statistics for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only
     * for statistic variables that provide time-based statistics, such as rate,
     * frequency and level).
     */
    void Summarize(bool updateTstamp) final;

    /**
     * @brief Prints statistics to log file using the given log level.
     * @param log_levelThe log level to use.
     */
    void Print(LogLevel logLevel) const final;

    /**
     * @brief Copies statistics from another object into this object.
     * @param rhs The right-hand-side object to copy from.
     */
    void Assign(const StatisticVariable& rhs) final;

    /**
     * @brief Adds statistics of another object to this object.
     * @param rhs The right-hand-side object to add from.
     */
    void Add(const StatisticVariable& rhs) final;

    /**
     * @brief Subtracts statistics of another object from this object.
     * @param rhs The right-hand-side subtrahend object.
     */
    void Subtract(const StatisticVariable& rhs) final;

    /**
     * @brief Divides the statistics of this object by a given factor.
     * @param factor The division factor.
     */
    void Divide(uint32_t factor) final;

    /**
     * @brief Resets all statistic values to zero.
     */
    void Reset() final;

    /**
     * @brief Adds a boolean sample.
     * @param value The sample value.
     */
    inline void AddSample(bool value)
    {
        m_sum += value ? 1 : 0;
        ++m_count;
    }

    /** @brief Retrieves the number of true-value smaples. */
    inline uint64_t GetTrueSampleCount() const
    {
        return m_sum;
    }

    /** @brief Retrieves the number of false-value smaples. */
    inline uint64_t GetFalseSampleCount() const
    {
        return m_count - m_sum;
    }

    /** @brief Retrieves the last computed percentage of true-value samples. */
    inline double GetPercentage() const
    {
        return m_percentage;
    }

private:
    /** @var Number of times 'true' was reported. */
    uint64_t m_sum;

    /** @var Last computed percentage. */
    double m_percentage;

    /** @var Last seen sample count. */
    uint64_t m_countSaved;
};
}  // namespace MOT

#endif /* BOOLEAN_STATISTIC_VARIABLE_H */
