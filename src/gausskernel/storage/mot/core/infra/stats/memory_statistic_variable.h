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
 * memory_statistic_variable.h
 *    A statistic variable for memory monitoring.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/memory_statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MEMORY_STATISTIC_VARIABLE_H
#define MEMORY_STATISTIC_VARIABLE_H

#include "statistic_variable.h"
#include "level_statistic_variable.h"
#include "rate_statistic_variable.h"
#include "cycles.h"

namespace MOT {
/**
 * @class MemoryStatisticVariable
 * @brief A statistic variable for memory monitoring. It computes the average (integral over time), peak and current
 * level, and also rate of de/allocation. In essence it is a combination of level and rate statistic variables.
 */
class MemoryStatisticVariable : public StatisticVariable {
public:
    /**
     * @brief Constructor.
     * @param name Name used for printing.
     * @param factor The scale-down factor to use in display.
     * @param units The unit name to use in display.
     */
    explicit MemoryStatisticVariable(const char* name = "", uint64_t factor = 1, const char* units = "")
        : StatisticVariable(name), m_level(name, factor, units), m_rate(name, factor, units)
    {}

    /** @brief Destructor. */
    ~MemoryStatisticVariable() override
    {}

    /**
     * @brief Configures string representation of the variable.
     * @param name Name used for printing.
     * @param factor The scale-down factor to use in display.
     * @param units The unit name to use in display.
     */
    inline void Configure(const char* name, uint64_t factor, const char* units)
    {
        m_level.Configure(name, factor, units);
        m_rate.Configure(name, factor, units);
    }

    /**
     * @brief Summarizes statistics for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    void Summarize(bool updateTstamp) override
    {
        m_level.Summarize(updateTstamp);
        m_rate.Summarize(updateTstamp);
    }

    /**
     * @brief Prints statistics to log file using the given log level.
     * @param logLevel The log level to use.
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
    inline void AddSample(int64_t value)
    {
        ++m_count;  // increase local sample count, otherwise it will not be printed
        m_level.AddSample(value);
        m_rate.AddSample(value);
    }

    /**
     * @brief Retrieves the current level.
     * @return The level.
     */
    inline uint64_t GetLevel() const
    {
        return m_level.GetLevel();
    }

    /**
     * @brief Retrieves the average of all the samples (integral).
     * @return The average level.
     */
    inline double GetAvg() const
    {
        return m_level.GetAvg();
    }

    /**
     * @brief Retrieves the rate during the last call to summarize.
     * @return The data rate.
     */
    inline double GetRate() const
    {
        return m_rate.GetRate();
    }

    /**
     * @brief Computes the up-to-date event frequency in the last measured interval (i.e. consecutive
     * calls to summarize()).
     * @return The up-to-date rate.
     */
    inline double GetUpToDateRate() const
    {
        return m_rate.GetUpToDateRate();
    }

private:
    /** @var The level statistics. */
    LevelStatisticVariable m_level;

    /** @var The rate statistics. */
    RateStatisticVariable m_rate;
};
}  // namespace MOT
#endif /* MEMORY_STATISTIC_VARIABLE_H */
