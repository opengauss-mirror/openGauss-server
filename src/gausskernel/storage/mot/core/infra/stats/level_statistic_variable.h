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
 * level_statistic_variable.h
 *    A statistic variable for level monitoring (e.g. memory consumption).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/level_statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LEVEL_STATISTICS_VARIABLE_H
#define LEVEL_STATISTICS_VARIABLE_H

#include "statistic_variable.h"
#include "cycles.h"

namespace MOT {
/**
 * @class LevelStatisticVariable
 * @brief A statistic variable for level monitoring (e.g. memory consumption). It computes the
 * average (integral over time), peak and current level.
 */
class LevelStatisticVariable : public StatisticVariable {
public:
    /**
     * @brief Constructor.
     * @param name Name used for printing.
     * @param factor The scale-down factor to use in display.
     * @param units The unit name to use in display.
     */
    explicit LevelStatisticVariable(const char* name = "", uint64_t factor = 1, const char* units = "")
        : StatisticVariable(name),
          m_factor(factor),
          m_level(0),
          m_peak(0),
          m_tstamp(0),
          m_initTstamp(0),
          m_integral(0),
          m_intervalSeconds(0),
          m_avg(0.0f),
          m_countSaved(0),
          m_levelSaved(0),
          m_peakSaved(0)
    {
        errno_t erc = snprintf_s(m_units, STAT_VAR_MAX_NAME_LEN, STAT_VAR_MAX_NAME_LEN - 1, "%s", units);
        securec_check_ss(erc, "\0", "\0");
    }

    /** @brief Destructor. */
    virtual ~LevelStatisticVariable()
    {}

    /**
     * @brief Configures string representation of the variable.
     * @param name Name used for printing.
     * @param factor The scale-down factor to use in display.
     * @param units The unit name to use in display.
     */
    inline void Configure(const char* name, uint64_t factor, const char* units)
    {
        SetName(name);
        m_factor = factor;
        errno_t erc = snprintf_s(m_units, STAT_VAR_MAX_NAME_LEN, STAT_VAR_MAX_NAME_LEN - 1, "%s", units);
        securec_check_ss(erc, "\0", "\0");
    }

    /**
     * @brief Summarizes statistics for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    void Summarize(bool updateTstamp) override;

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
        ++m_count;
        uint64_t prevTstamp = m_tstamp;
        m_tstamp = CpuCyclesLevelTime::Rdtscp();
        if (m_initTstamp == 0) {
            m_initTstamp = m_tstamp;
        } else {
            m_integral += m_level * (m_tstamp - prevTstamp);
        }
        m_level += value;
        if (m_level > m_peak) {
            m_peak = m_level;
        }
    }

    /**
     * @brief Retrieves the current level.
     * @return The level.
     */
    inline uint64_t GetLevel() const
    {
        return m_level;
    }

    /**
     * @brief Retrieves the average of all the samples (integral).
     * @return The average level.
     */
    inline double GetAvg() const
    {
        return m_avg;
    }

private:
    /** @var Scale-down factor to use in display. */
    uint64_t m_factor;

    /** @var Unit name to use in display. */
    char m_units[STAT_VAR_MAX_NAME_LEN];

    /** @var The current level of the variable. */
    int64_t m_level __attribute__((aligned(64)));

    /** @var The peak level seen in all history. */
    int64_t m_peak __attribute__((aligned(64)));

    /** @var Time stamp of last recorded event. */
    uint64_t m_tstamp __attribute__((aligned(64)));

    /** @var Initial time stamp starting from the first recorded event. */
    uint64_t m_initTstamp __attribute__((aligned(64)));

    /** @var The square sum of all the samples. */
    uint64_t m_integral __attribute__((aligned(64)));

    /** @var The time interval in seconds. */
    double m_intervalSeconds __attribute__((aligned(64)));

    /** @var The average of all the samples. */
    double m_avg __attribute__((aligned(64)));

    /** @var Last seen sample count. */
    uint64_t m_countSaved;

    /** @var Last seen level. */
    int64_t m_levelSaved;

    /** @var Last seen peak. */
    uint64_t m_peakSaved;
};
}  // namespace MOT
#endif /* LEVEL_STATISTICS_VARIABLE_H */
