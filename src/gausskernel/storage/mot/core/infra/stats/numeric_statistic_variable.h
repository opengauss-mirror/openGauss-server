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
 * numeric_statistic_variable.h
 *    A statistic variable for integral types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/numeric_statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NUMERIC_STATISTIC_VARIABLE_H
#define NUMERIC_STATISTIC_VARIABLE_H

#include "statistic_variable.h"

#include <climits>

namespace MOT {
/**
 * @class NumericStatisticVariable
 * @brief A statistic variable for integral types. It computes average and standard deviation.
 */
class NumericStatisticVariable : public StatisticVariable {
public:
    /**
     * @brief Constructor.
     * @param name Name used for printing.
     * @param factor The scale-down factor to use in display.
     * @param units The unit name to use in display.
     * @param limit Sample value limit. Samples exceeding this limit are filtered out.
     */
    explicit NumericStatisticVariable(
        const char* name, uint64_t factor = 1, const char* units = "", uint64_t limit = ULONG_LONG_MAX);

    /** @brief Destructor. */
    ~NumericStatisticVariable() override
    {}

    /**
     * @brief Summarizes statistics for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    void Summarize(bool updateTstamp) override;

    /**
     * @brief Prints statistics to log file using the given log level.
     * @param log_levelThe log level to use.
     */
    void Print(LogLevel logLevel) const override;

    /**
     * @brief Copies statistics from another object into this object.
     * @param rhs The right-hand-side object to copy from.
     */
    void Assign(const StatisticVariable& rhs) override;

    /**
     * @brief Adds statistics of another object to this object.
     * @param rhs The right-hand-side object to add from.
     */
    void Add(const StatisticVariable& rhs) override;

    /**
     * @brief Subtracts statistics of another object from this object.
     * @param rhs The right-hand-side subtrahend object.
     */
    void Subtract(const StatisticVariable& rhs) override;

    /**
     * @brief Divides the statistics of this object by a given factor.
     * @param factor The division factor.
     */
    void Divide(uint32_t factor) override;

    /**
     * @brief Resets all statistic values to zero.
     */
    void Reset() override;

    /**
     * @brief Adds a sample to the statistics.
     * @param value The sample value
     */
    inline void AddSample(uint64_t value)
    {
        if (value <= m_limit) {
            m_sum += value;
            m_squareSum += value * value;
            ++m_count;
        }
    }

    /**
     * @brief Retrieves the sum of all the samples.
     * @return The sum.
     */
    inline uint64_t GetSum() const
    {
        return m_sum;
    }

    /**
     * @brief Retrieves the average of all the samples.
     * @return The average.
     */
    inline double GetAvg() const
    {
        return m_avg;
    }

    /**
     * @brief Retrieves the variance of all the samples.
     * @return The variance.
     */
    inline double GetVar() const
    {
        return m_var;
    }

    /**
     * @brief Retrieves the standard deviation of all the samples.
     * @return The standard deviation.
     */
    inline double GetStd() const
    {
        return m_std;
    }

private:
    /** @var Scale-down factor to use in display. */
    const uint64_t m_factor;

    /** @var Unit name to use in display. */
    char m_units[STAT_VAR_MAX_NAME_LEN];

    /** @var Sample value limit. */
    const uint64_t m_limit;

    /** @var The sum of all the samples. */
    uint64_t m_sum;

    /** @var The square sum of all the samples. */
    uint64_t m_squareSum;

    /** @var The average of all the samples. */
    double m_avg;

    /** @var The variance of all the samples. */
    double m_var;

    /** @var The standard deviation of all the samples. */
    double m_std;

    /** @var Last seen sample count. */
    uint64_t m_countSaved;
};
}  // namespace MOT
#endif /* NUMERIC_STATISTIC_VARIABLE_H */
